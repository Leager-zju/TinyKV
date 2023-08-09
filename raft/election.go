package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

func (r *Raft) electionTimeoutEvent() {
	r.resetElectionElapsed()
	for peer := range r.Prs {
		if peer != r.id {
			r.sendRequestVote(peer)
		}
	}
}

// sendHeartbeat sends a RequestVote RPC to the given peer.
func (r *Raft) sendRequestVote(to uint64) {
	// DPrintf("[%d] send request vote to %d", r.id, to)
	r.sendNewMsg(&pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: r.RaftLog.LastTerm(),
		Index:   r.RaftLog.LastIndex(),
		Commit:  r.RaftLog.committed,
	})
}

// handleRequestVote handle RequestVote RPC request
func (r *Raft) handleRequestVote(m pb.Message) {
	// DPrintf("[%d] handle {%s}", r.id, m.String())
	response := &pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	}
	defer r.sendNewMsg(response)

	if m.GetTerm() < r.Term { // Err Old Term
		response.Reject = true
		DPrintf("[%s] ErrOldTerm", RaftToString(r))
		return
	}

	if m.GetTerm() == r.Term && (r.Vote != None && r.Vote != m.GetFrom()) { // Err Already Voted
		response.Reject = true
		DPrintf("[%s] ErrAlreadyVoted", RaftToString(r))
		return
	}

	if m.GetTerm() > r.Term {
		r.becomeFollower(m.Term, None)
		response.Term = r.Term
	}

	if r.RaftLog.LastTerm() > m.GetLogTerm() || (r.RaftLog.LastTerm() == m.GetLogTerm() && r.RaftLog.LastIndex() > m.GetIndex()) {
		response.Reject = true
		DPrintf("[%s] ErrOldLog", RaftToString(r))
		return
	}

	r.Vote = m.GetFrom()
	r.votes[m.GetFrom()] = true
	r.resetElectionElapsed()
}

// handleRequestVoteResponse handle RequestVoteResponse RPC request
func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.GetTerm() > r.Term {
		r.becomeFollower(m.GetTerm(), None)
		return
	}

	if m.GetTerm() == r.Term && r.State == StateCandidate {
		if m.GetReject() {
			r.rejectCnt++
			if r.rejectCnt > r.getQuorum() {
				r.becomeFollower(r.Term, None)
			}
		} else {
			r.voteCnt++
			if r.voteCnt > r.getQuorum() {
				r.becomeLeader()
			}
		}
	}
}
