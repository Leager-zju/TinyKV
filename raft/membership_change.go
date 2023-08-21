package raft

import "github.com/pingcap-incubator/tinykv/log"

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	if _, ok := r.Prs[id]; !ok {

		r.Prs[id] = &Progress{
			Match: r.RaftLog.TruncatedIndex(),
			Next:  r.RaftLog.LastIndex() + 1,
		}
		r.votes[id] = false
		log.Infof("[%s] AddNode %d. Peers: %+v", RaftToString(r), id, r.Prs)
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	if _, ok := r.Prs[id]; ok {
		delete(r.Prs, id)
		delete(r.votes, id)
		log.Infof("[%s] DeleteNode %d. Peers: %+v", RaftToString(r), id, r.Prs)
	}
}
