package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func (r *Raft) handleTransferLeader(m pb.Message) {
	if r.State != StateLeader && m.GetFrom() == r.id {
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgTimeoutNow,
			To:      r.id,
			From:    r.id,
			Term:    r.Term,
		})
	}

	if r.State == StateLeader && m.GetFrom() != r.id {
		if pr, ok := r.Prs[m.GetFrom()]; ok {
			r.leadTransferee = m.GetFrom()
			// 1. check the qualification of the transferee
			if pr.Next != r.RaftLog.LastIndex()+1 {
				r.sendAppend(m.GetFrom())
				return
			}

			// 2. if conditions are met, send time out
			r.becomeFollower(r.Term, m.GetFrom())
			r.sendTimeout(m.GetFrom())
		}
	}
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
	if _, ok := r.Prs[r.id]; !ok {
		return
	}

	if m.GetTerm() < r.Term { // Err Old Term
		return
	}

	if m.GetTerm() > r.Term || (m.GetTerm() == r.Term && r.State == StateCandidate) {
		r.becomeFollower(m.GetTerm(), m.GetFrom())
	}

	r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
}
