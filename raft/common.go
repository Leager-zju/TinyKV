package raft

import (
	"fmt"
	"math/rand"

	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

const DEBUG bool = true

func DPrintf(format string, a ...interface{}) {
	if DEBUG {
		log.Infof(format, a...)
	}
}

func RaftToString(r *Raft) string {
	return fmt.Sprintf("%d %d %s [%d..%d..%d..%d..%d]", r.id, r.Term, r.State.String(), r.RaftLog.TruncatedIndex(), r.RaftLog.applied, r.RaftLog.committed, r.RaftLog.stabled, r.RaftLog.LastIndex())
}

func MessageToString(m pb.Message) string {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		return fmt.Sprintf("{Type: %s}", pb.MessageType_name[int32(m.GetMsgType())])
	case pb.MessageType_MsgBeat:
		return fmt.Sprintf("{Type: %s}", pb.MessageType_name[int32(m.GetMsgType())])
	case pb.MessageType_MsgPropose:
		return fmt.Sprintf("{Type: %s, Entries: %v}", pb.MessageType_name[int32(m.GetMsgType())], m.GetEntries())
	case pb.MessageType_MsgAppend:
		return fmt.Sprintf("{Type: %s, Term: %d, From: %d, To: %d, PrevLogTerm: %d, PrevLogIndex: %d, LeaderCommit: %d, Entries: %v}", pb.MessageType_name[int32(m.GetMsgType())], m.GetTerm(), m.GetFrom(), m.GetTo(), m.GetLogTerm(), m.GetIndex(), m.GetCommit(), m.GetEntries())
	case pb.MessageType_MsgAppendResponse:
		return fmt.Sprintf("{Type: %s, Term: %d, From: %d, To: %d, ConflictLogTerm: %d, ConflictLogIndex: %d, Reject: %t}", pb.MessageType_name[int32(m.GetMsgType())], m.GetTerm(), m.GetFrom(), m.GetTo(), m.GetLogTerm(), m.GetIndex(), m.GetReject())
	case pb.MessageType_MsgRequestVote:
		return fmt.Sprintf("{Type: %s, Term: %d, From: %d, To: %d, LastLogTerm: %d, LastLogIndex: %d}", pb.MessageType_name[int32(m.GetMsgType())], m.GetTerm(), m.GetFrom(), m.GetTo(), m.GetLogTerm(), m.GetIndex())
	case pb.MessageType_MsgRequestVoteResponse:
		return fmt.Sprintf("{Type: %s, Term: %d, From: %d, To: %d, Reject: %t}", pb.MessageType_name[int32(m.GetMsgType())], m.GetTerm(), m.GetFrom(), m.GetTo(), m.GetReject())
	case pb.MessageType_MsgSnapshot:
		return fmt.Sprintf("{Type: %s, Term: %d, From: %d, To: %d, LastIncludedTerm: %d, LastIncludedIndex: %d}", pb.MessageType_name[int32(m.GetMsgType())], m.GetTerm(), m.GetFrom(), m.GetTo(), m.GetSnapshot().GetMetadata().GetTerm(), m.GetSnapshot().GetMetadata().GetIndex())
	case pb.MessageType_MsgHeartbeat:
		return fmt.Sprintf("{Type: %s, Term: %d, From: %d, To: %d, Committed: %d}", pb.MessageType_name[int32(m.GetMsgType())], m.GetTerm(), m.GetFrom(), m.GetTo(), m.GetCommit())
	case pb.MessageType_MsgHeartbeatResponse:
		return fmt.Sprintf("{Type: %s, Term: %d, From: %d, To: %d, ConflictLogIndex: %d, Reject: %t}", pb.MessageType_name[int32(m.GetMsgType())], m.GetTerm(), m.GetFrom(), m.GetTo(), m.GetIndex(), m.GetReject())
	case pb.MessageType_MsgTransferLeader:
		return fmt.Sprintf("{Type: %s, Transferee: %d}", pb.MessageType_name[int32(m.GetMsgType())], m.GetFrom())
	case pb.MessageType_MsgTimeoutNow:
		return fmt.Sprintf("{Type: %s, Term: %d, From: %d, To: %d}", pb.MessageType_name[int32(m.GetMsgType())], m.GetTerm(), m.GetFrom(), m.GetTo())
	}
	return ""
}

func (r *Raft) getQuorum() uint64 {
	return uint64(len(r.Prs)) / 2
}

func (r *Raft) getElectionTimeout() int {
	return rand.Intn(2*r.electionTimeout) + r.electionTimeout
}

func (r *Raft) getHeartbeatTimeout() int {
	return r.heartbeatTimeout
}

func (r *Raft) resetElectionElapsed() {
	r.electionElapsed = 0
}

func (r *Raft) resetHeartbeatElapsed() {
	r.heartbeatElapsed = 0
}

func (r *Raft) clearVote() {
	r.Vote = None
	r.voteCnt = 0
	r.rejectCnt = 0
	for peer := range r.votes {
		r.votes[peer] = false
	}
}

func (r *Raft) updateCommitted() {
	var N uint64 = r.RaftLog.TruncatedIndex()
	for peer := range r.Prs {
		N = max(N, r.Prs[peer].Match)
	}

	for ; N > r.RaftLog.committed; N-- {
		if r.RaftLog.entries[r.RaftLog.Index2idx(N)].GetTerm() != r.Term {
			continue
		}

		cnt := 0
		for _, pr := range r.Prs {
			if pr.Match >= N {
				cnt++
			}
		}

		if cnt > int(r.getQuorum()) {
			r.RaftLog.committed = N
			r.startAppend()
			return
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.Term, r.State, r.Lead = term, StateFollower, lead
	r.leadTransferee = None
	r.clearVote()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// On conversion to candidate, start election:
	//  • Increment currentTerm
	//  • Vote for self
	//  • Reset election timer
	//  • Send RequestVote RPCs to all other servers
	r.State = StateCandidate
	r.Term++
	r.Lead = r.id
	r.Vote = r.id
	r.voteCnt = 1
	r.votes[r.id] = true
	r.resetElectionElapsed()

	if r.voteCnt > r.getQuorum() {
		r.becomeLeader()
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.resetHeartbeatElapsed()
	r.proposeNoopEntry()
}

func getLastLogIndex(m *pb.Message) uint64 {
	n := len(m.GetEntries())
	if n > 0 {
		return m.Entries[n-1].GetIndex()
	}
	return m.GetIndex()
}
