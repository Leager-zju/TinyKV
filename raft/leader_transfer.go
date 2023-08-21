package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) handleTransferLeader(m pb.Message) {
	if m.GetFrom() == r.id {
		return
	}

	r.leadTransferee = m.GetFrom()
	// 1. check the qualification of the transferee
	if r.Prs[m.GetFrom()].Next != r.RaftLog.LastIndex()+1 {
		r.sendAppend(m.GetFrom())
		return
	}

	// 2. send time out
	r.sendTimeout(m.GetFrom())
}

func (r *Raft) sendTimeout(to uint64) {
	r.sendNewMsg(&pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

func (r *Raft) handleTimeoutNow(m pb.Message) {
	if m.GetTerm() < r.Term { // Err Old Term
		return
	}

	if m.GetTerm() > r.Term || (m.GetTerm() == r.Term && r.State == StateCandidate) {
		r.becomeFollower(m.GetTerm(), m.GetFrom())
	}

	r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
}
