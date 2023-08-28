package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) startAppend() {
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}

		r.sendAppend(peer)
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	nextIndex := r.Prs[to].Next
	prevIndex := nextIndex - 1

	if nextIndex <= r.RaftLog.TruncatedIndex() {
		r.sendSnapshot(to)
		return true
	}

	request := &pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	defer r.sendNewMsg(request)

	for idx := r.RaftLog.Index2idx(nextIndex); idx < r.RaftLog.length(); idx++ {
		request.Entries = append(request.Entries, &r.RaftLog.entries[idx])
	}
	request.LogTerm, _ = r.RaftLog.Term(prevIndex)
	request.Index = prevIndex
	return true
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// r.updateCommitted()
	response := &pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    r.id,
		Term:    r.Term,
		Reject:  false,
	}
	defer r.sendNewMsg(response)

	if m.GetTerm() < r.Term { // Err Old Term
		response.Reject = true
		return
	}

	if m.GetTerm() > r.Term || (m.GetTerm() == r.Term && r.State == StateCandidate) {
		r.becomeFollower(m.GetTerm(), m.GetFrom())
		response.Term = r.Term
	}

	prevLogIndex, prevLogTerm := m.GetIndex(), m.GetLogTerm()
	if prevLogIndex > r.RaftLog.LastIndex() {
		response.Reject = true
		return
	}

	term, err := r.RaftLog.Term(prevLogIndex)
	if err != nil {
		log.Panic(err)
	}
	if term != prevLogTerm && prevLogIndex != 0 { // Err Log Doesn't Match
		response.Reject = true
		return
	}

	lastNewLogIndex := getLastLogIndex(&m)
	if len(m.Entries) > 0 {
		baseNewLogIndex := m.Entries[0].GetIndex()
		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it
		newLogIndex := baseNewLogIndex
		for ; newLogIndex <= min(r.RaftLog.LastIndex(), lastNewLogIndex); newLogIndex++ {
			newLogTerm, err := r.RaftLog.Term(newLogIndex)
			if err != nil {
				log.Panic(err)
			}
			if newLogTerm != m.Entries[newLogIndex-baseNewLogIndex].GetTerm() {
				r.RaftLog.entries = r.RaftLog.entries[:r.RaftLog.Index2idx(newLogIndex)]
				r.RaftLog.stabled = min(r.RaftLog.stabled, newLogIndex-1)
				break
			}
		}
		for ; newLogIndex <= lastNewLogIndex; newLogIndex++ {
			r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[newLogIndex-baseNewLogIndex])
		}
	}

	// if len(m.Entries) == 0
	//    the follower only be sure it has the same Entry
	//    {Term: prev log term, Index: prev log index},
	//    so the committed can't exceed "prev log index",
	//    and the last log index is "prev log index"
	// else
	//    update committed as normal
	if m.GetCommit() > r.RaftLog.committed {
		r.RaftLog.committed = min(m.GetCommit(), lastNewLogIndex)
	}
	response.Index = lastNewLogIndex

	r.Lead = m.GetFrom()
	r.clearVote()
	r.resetElectionElapsed()
}

// handleAppendEntriesResponse handle AppendEntriesResponse RPC request
func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.GetTerm() > r.Term {
		r.becomeFollower(m.GetTerm(), None)
		return
	}

	if m.GetTerm() == r.Term && r.State == StateLeader { // prevent old rpc
		if m.GetReject() { // prev log conlicts with follower, decrease NEXT and retry
			r.Prs[m.GetFrom()].Next--
			r.sendAppend(m.GetFrom())
		} else { // update MATCH with last log infomation of follower, and LeaderCommit
			r.Prs[m.GetFrom()].Match = m.GetIndex()
			r.Prs[m.GetFrom()].Next = m.GetIndex() + 1
			r.updateCommitted()

			if m.GetFrom() == r.leadTransferee && r.Prs[m.GetFrom()].Next == r.RaftLog.LastIndex()+1 {
				r.becomeFollower(r.Term, m.GetFrom())
				r.sendTimeout(m.GetFrom())
				return
			}
		}
	}
}

func (r *Raft) heartbeatTimeoutEvent() {
	r.resetHeartbeatElapsed()
	for peer := range r.Prs {
		if peer == r.id {
			continue
		}

		r.sendHeartbeat(peer)
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	msg := &pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}

	if pr := r.Prs[to]; pr.Match == 0 {
		msg.Commit = 0
	}
	r.sendNewMsg(msg)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	response := &pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      m.GetFrom(),
		From:    r.id,
		Term:    r.Term,
		Index:   0,
	}
	defer r.sendNewMsg(response)

	if m.GetTerm() < r.Term {
		response.Reject = true
		return
	}

	if m.GetTerm() > r.Term || (m.GetTerm() == r.Term && r.State == StateCandidate) {
		r.becomeFollower(m.GetTerm(), m.GetFrom())
		response.Term = r.Term
	}

	if m.GetCommit() > r.RaftLog.LastIndex() { // commit index over flow
		response.Reject = true
		response.Index = r.RaftLog.LastIndex()
		return
	}

	if m.GetCommit() > r.RaftLog.committed {
		committed := min(m.GetCommit(), r.RaftLog.LastIndex())
		term, err := r.RaftLog.Term(committed)
		if err != nil {
			log.Panic(err)
		}
		if term == r.Term {
			r.RaftLog.committed = committed
		}
	}

	r.Lead = m.GetFrom()
	r.clearVote()
	r.resetElectionElapsed()
}

// handleHeartbeatResponse handle HeartbeatResponse RPC request
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if m.GetTerm() > r.Term {
		r.becomeFollower(m.GetTerm(), None)
	}

	if m.GetTerm() == r.Term && r.State == StateLeader && m.GetIndex() < r.RaftLog.committed {
		r.sendAppend(m.GetFrom())
	}
}

func (r *Raft) proposeNoopEntry() {
	r.proposeEntry(pb.Message{
		Entries: []*pb.Entry{{Term: r.Term, Index: r.RaftLog.LastIndex() + 1}},
	})
}

func (r *Raft) proposeEntry(m pb.Message) {
	for _, entry := range m.GetEntries() {
		newEntry := pb.Entry{
			EntryType: entry.GetEntryType(),
			Term:      r.Term,
			Index:     r.RaftLog.LastIndex() + 1,
			Data:      entry.GetData(),
		}
		DPrintf("[%s] Propose %+v", RaftToString(r), newEntry)

		if entry.GetEntryType() == pb.EntryType_EntryConfChange {
			r.PendingConfIndex = newEntry.GetIndex()
		}
		r.RaftLog.appendEntry(newEntry)
	}

	if _, ok := r.Prs[r.id]; ok {
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	}

	if len(r.Prs) <= 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	} else {
		r.startAppend()
	}
}
