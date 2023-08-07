package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

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
	request := &pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	}
	defer r.sendNewMsg(request)

	next_idx := r.Prs[to].Next
	prev_idx := next_idx - 1

	for idx := next_idx; idx < uint64(len(r.RaftLog.entries)); idx++ {
		request.Entries = append(request.Entries, &r.RaftLog.entries[idx])
	}
	request.LogTerm, _ = r.RaftLog.Term(prev_idx)
	request.Index = prev_idx
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

	prev_log_index, prev_log_term := m.GetIndex(), m.GetLogTerm()
	term, _ := r.RaftLog.Term(prev_log_index)
	if prev_log_index > r.RaftLog.LastIndex() || (term != prev_log_term && prev_log_index != 0) { // Err Log Doesn't Match
		response.Reject = true
		return
	}

	last_new_log_index := getLastLogIndex(&m)
	if len(m.Entries) > 0 {
		base_new_log_index := m.Entries[0].GetIndex()
		// If an existing entry conflicts with a new one (same index
		// but different terms), delete the existing entry and all that
		// follow it
		new_log_index := base_new_log_index
		for ; new_log_index <= min(r.RaftLog.LastIndex(), last_new_log_index); new_log_index++ {
			new_log_term, _ := r.RaftLog.Term(new_log_index)
			if new_log_term != m.Entries[new_log_index-base_new_log_index].GetTerm() {
				r.RaftLog.entries = r.RaftLog.entries[:new_log_index]
				r.RaftLog.stabled = min(r.RaftLog.stabled, new_log_index-1)
				break
			}
		}
		for ; new_log_index <= last_new_log_index; new_log_index++ {
			r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[new_log_index-base_new_log_index])
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
		r.RaftLog.committed = min(m.GetCommit(), last_new_log_index)
	}
	response.Index = last_new_log_index

	r.Lead = m.GetFrom()
	r.clearVote()
	r.resetElectionElapsed()
	r.updateReady()
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
		}
	}
}

func (r *Raft) startHeartbeat() {
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
		if term, _ := r.RaftLog.Term(committed); term == r.Term {
			r.RaftLog.committed = committed
		}
	}

	r.Lead = m.GetFrom()
	r.clearVote()
	r.resetElectionElapsed()
	r.updateReady()
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
		new_entry := pb.Entry{
			EntryType: entry.GetEntryType(),
			Term:      r.Term,
			Index:     r.RaftLog.LastIndex() + 1,
			Data:      entry.GetData(),
		}
		r.RaftLog.appendEntry(new_entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	r.updateReady()
	if len(r.Prs) == 1 {
		r.updateCommitted()
	} else {
		r.startAppend()
	}
}
