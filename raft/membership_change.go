package raft

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	if _, ok := r.Prs[id]; !ok {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  0,
		}
		r.votes[id] = false
		DPrintf("[%s] AddNode %d. Peers: %+v", RaftToString(r), id, r.Prs)
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		delete(r.votes, id)
		r.updateCommitted()
		DPrintf("[%s] DeleteNode %d. Peers: %+v", RaftToString(r), id, r.Prs)
	}
}
