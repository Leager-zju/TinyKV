package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) updateStoreMeta(peer *peer, prev, cur *metapb.Region) {
	Meta := d.ctx.storeMeta
	Meta.Lock()
	defer Meta.Unlock()
	if prev != nil {
		Meta.regionRanges.Delete(&regionItem{region: prev})
	}
	Meta.regionRanges.ReplaceOrInsert(&regionItem{region: cur})
	DPrintf("%s\n\t\tDelete {%+v}\n\t\tInsert {%+v}", d.Tag, prev, cur)
	Meta.setRegion(cur, peer)
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	if d.RaftGroup.HasReady() {
		ready := d.RaftGroup.Ready()

		// 1. send messages
		d.Send(d.ctx.trans, ready.Messages)

		// 2. persist update
		applySnapResult, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			log.Panic(err)
		}
		if applySnapResult != nil {
			wb := new(engine_util.WriteBatch)
			d.updateStoreMeta(d.peer, applySnapResult.PrevRegion, applySnapResult.Region)
			meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
			wb.MustWriteToDB(d.peerStorage.Engines.Kv)
		}

		// 3. apply entries (防止因 apply entries 导致 region epoch 改变，从而 snapshot 失效)
		if n := len(ready.CommittedEntries); n > 0 {
			for _, entry := range ready.CommittedEntries {
				// 这里不是最后统一写入 badger 是防止出现先 put 再 get，由于最后统一写而 get 不到的情况
				kvWB := new(engine_util.WriteBatch)
				if entry.EntryType == eraftpb.EntryType_EntryConfChange {
					d.applyConfChange(&entry, kvWB)
				} else {
					d.applyCommittedEntry(&entry, kvWB)
				}
				if d.stopped { // 存在 destroy self 的可能性，一旦发生，后面都不用做了
					return
				}
				d.peerStorage.applyState.AppliedIndex = entry.GetIndex()
				if err := kvWB.SetMeta(meta.ApplyStateKey(d.Region().GetId()), d.peerStorage.applyState); err != nil {
					log.Panic(err)
				}
				kvWB.MustWriteToDB(d.ctx.engine.Kv)
			}
		}

		// 4. tell raft module to advance
		d.RaftGroup.Advance(ready)
	}
}

func (d *peerMsgHandler) applyConfChange(entry *eraftpb.Entry, wb *engine_util.WriteBatch) {
	cc := new(eraftpb.ConfChange)
	if err := cc.Unmarshal(entry.GetData()); err != nil {
		log.Panic(err)
	}
	msg := new(raft_cmdpb.RaftCmdRequest)
	if err := msg.Unmarshal(cc.GetContext()); err != nil {
		log.Panic(err)
	}

	DPrintf("%s Try Apply ConfChange{%+v} At {index: %d, term: %d}", d.Tag, msg, entry.GetIndex(), entry.GetTerm())
	if msg.GetHeader().GetRegionId() != d.regionId {
		DPrintf("%s command {%+v} ERROR region id not found ", d.Tag, msg)
		d.SendResponse(ErrRespRegionNotFound(msg.GetHeader().GetRegionId()), entry.GetIndex(), entry.GetTerm(), false)
		return
	}
	if err, ok := util.CheckRegionEpoch(msg, d.Region(), true).(*util.ErrEpochNotMatch); ok {
		DPrintf("%s command {%+v} ERROR %+v", d.Tag, cc, err)
		d.SendResponse(ErrResp(err), entry.GetIndex(), entry.GetTerm(), false)
		return
	}

	{
		// modify storeMeta.region.RegionEpoch
		newRegion := new(metapb.Region)
		if err := util.CloneMsg(d.Region(), newRegion); err != nil {
			log.Panic(err)
		}
		newRegion.RegionEpoch.ConfVer++
		// modify storeMeta.region.Peers and peer.peerCache
		switch cc.GetChangeType() {
		case eraftpb.ConfChangeType_AddNode:
			// if not exist, add it; otherwise not
			exist := false
			for _, peer := range newRegion.GetPeers() {
				if peer.GetId() == cc.GetNodeId() {
					exist = true
					break
				}
			}
			if !exist {
				newPeer := msg.GetAdminRequest().GetChangePeer().GetPeer()
				newRegion.Peers = append(newRegion.GetPeers(), newPeer)
				d.insertPeerCache(newPeer)
			}
		case eraftpb.ConfChangeType_RemoveNode:
			// if exist, remove it; otherwise not
			for i, peer := range newRegion.GetPeers() {
				if peer.GetId() == cc.GetNodeId() {
					if d.PeerId() == cc.GetNodeId() {
						d.destroyPeer()
						return
					}
					newRegion.Peers = append(newRegion.Peers[:i], newRegion.Peers[i+1:]...)
					break
				}
			}
			d.removePeerCache(cc.GetNodeId())
		default:
			log.Error("invalid change type")
		}

		d.updateStoreMeta(d.peer, d.Region(), newRegion)
		meta.WriteRegionState(wb, d.Region(), rspb.PeerState_Normal)
	}

	d.RaftGroup.ApplyConfChange(*cc)
	if d.IsLeader() {
		d.SendResponse(&raft_cmdpb.RaftCmdResponse{
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
				ChangePeer: &raft_cmdpb.ChangePeerResponse{
					Region: d.Region(),
				},
			},
		}, entry.GetIndex(), entry.GetTerm(), false)
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
}

func (d *peerMsgHandler) applyCommittedEntry(entry *eraftpb.Entry, wb *engine_util.WriteBatch) {
	msg := new(raft_cmdpb.RaftCmdRequest)
	if err := msg.Unmarshal(entry.GetData()); err != nil {
		log.Panic(err)
	}

	DPrintf("%s Try Apply Command{%+v} At {index: %d, term: %d}", d.Tag, msg, entry.GetIndex(), entry.GetTerm())
	if msg.GetHeader().GetRegionId() != d.regionId {
		DPrintf("%s command {%+v} ERROR region id not found ", d.Tag, msg)
		d.SendResponse(ErrRespRegionNotFound(msg.GetHeader().GetRegionId()), entry.GetIndex(), entry.GetTerm(), false)
		return
	}
	if err, ok := util.CheckRegionEpoch(msg, d.Region(), true).(*util.ErrEpochNotMatch); ok {
		DPrintf("%s command {%+v} ERROR %+v", d.Tag, msg, err)
		d.SendResponse(ErrResp(err), entry.GetIndex(), entry.GetTerm(), false)
		return
	}

	if len(msg.GetRequests()) > 0 {
		d.applyNormalCommand(msg, entry, wb)
	}

	if msg.GetAdminRequest() != nil {
		d.applyAdminCommand(msg, entry, wb)
	}
}

func (d *peerMsgHandler) applyNormalCommand(msg *raft_cmdpb.RaftCmdRequest, entry *eraftpb.Entry, wb *engine_util.WriteBatch) {
	isSnap := false
	response := newCmdResp()
	for _, req := range msg.GetRequests() {
		switch req.GetCmdType() {
		case raft_cmdpb.CmdType_Get:
			getReq := req.GetGet()
			cf, key := getReq.GetCf(), getReq.GetKey()
			if err, ok := util.CheckKeyInRegion(key, d.Region()).(*util.ErrKeyNotInRegion); ok {
				response = ErrResp(err)
				break
			}
			value, err := engine_util.GetCF(d.peerStorage.Engines.Kv, cf, key)
			if err != nil {
				log.Panic(fmt.Sprintf("%s Err: %s", d.Tag, err))
			}
			response.Responses = append(response.GetResponses(), &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Get,
				Get: &raft_cmdpb.GetResponse{
					Value: value,
				},
			})
		case raft_cmdpb.CmdType_Put:
			putReq := req.GetPut()
			cf, key, value := putReq.GetCf(), putReq.GetKey(), putReq.GetValue()
			if err, ok := util.CheckKeyInRegion(key, d.Region()).(*util.ErrKeyNotInRegion); ok {
				response = ErrResp(err)
				break
			}
			wb.SetCF(cf, key, value)
			response.Responses = append(response.GetResponses(), &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Put,
				Put:     &raft_cmdpb.PutResponse{},
			})
		case raft_cmdpb.CmdType_Delete:
			deleteReq := req.GetDelete()
			cf, key := deleteReq.GetCf(), deleteReq.GetKey()
			if err, ok := util.CheckKeyInRegion(key, d.Region()).(*util.ErrKeyNotInRegion); ok {
				response = ErrResp(err)
				break
			}
			wb.DeleteCF(cf, key)
			response.Responses = append(response.GetResponses(), &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Delete,
				Delete:  &raft_cmdpb.DeleteResponse{},
			})
		case raft_cmdpb.CmdType_Snap: // it seems that it just returns Region
			isSnap = true
			response.Responses = append(response.GetResponses(), &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap: &raft_cmdpb.SnapResponse{
					Region: d.Region(),
				},
			})
		case raft_cmdpb.CmdType_Invalid:
			log.Info("Invalid CmdType")
		}
	}

	if d.IsLeader() {
		d.SendResponse(response, entry.GetIndex(), entry.GetTerm(), isSnap)
		// bug record:
		// 原 leader 可能已经 apply 了一个 confchange，然后因为要 remove self，transfer leader 给当前 peer
		// 新 leader 可能在 apply conf change 之前触发 heartbeatScheduler 导致 stale region epoch
		// d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
}

func (d *peerMsgHandler) applyAdminCommand(msg *raft_cmdpb.RaftCmdRequest, entry *eraftpb.Entry, wb *engine_util.WriteBatch) {
	req := msg.GetAdminRequest()
	switch req.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		compactLogIndex := req.GetCompactLog().GetCompactIndex()
		compactLogTerm := req.GetCompactLog().GetCompactTerm()
		// 1. do the actual log deletion work
		d.ScheduleCompactLog(compactLogIndex)

		// 2. update applystate
		if d.peerStorage.applyState.TruncatedState.GetIndex() < compactLogIndex {
			d.peerStorage.applyState.TruncatedState.Index = compactLogIndex
			d.peerStorage.applyState.TruncatedState.Term = compactLogTerm
		}
	case raft_cmdpb.AdminCmdType_Split:
		response := newCmdResp()
		split := req.GetSplit()
		if err, ok := util.CheckKeyInRegion(split.GetSplitKey(), d.Region()).(*util.ErrKeyNotInRegion); ok {
			DPrintf("%s command {%+v} ERROR %+v", d.Tag, msg, err)
			response = ErrResp(err)
			d.SendResponse(response, entry.GetIndex(), entry.GetTerm(), false)
			return
		}
		// 1. split to 2 different Region
		newRegion1 := &metapb.Region{ // [startKey: splitKey)
			Id:       d.regionId,
			StartKey: d.Region().GetStartKey(),
			EndKey:   split.GetSplitKey(),
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: msg.GetHeader().GetRegionEpoch().GetConfVer(),
				Version: msg.GetHeader().GetRegionEpoch().GetVersion() + 1,
			},
			Peers: d.Region().GetPeers(),
		}
		newRegion2 := &metapb.Region{ // [splitKey: endKey)
			Id:       split.GetNewRegionId(),
			StartKey: split.GetSplitKey(),
			EndKey:   d.Region().GetEndKey(),
			RegionEpoch: &metapb.RegionEpoch{
				ConfVer: msg.GetHeader().GetRegionEpoch().GetConfVer(),
				Version: msg.GetHeader().GetRegionEpoch().GetVersion() + 1,
			},
		}
		for i, id := range split.GetNewPeerIds() {
			newRegion2.Peers = append(newRegion2.Peers, &metapb.Peer{
				Id:      id,
				StoreId: d.Region().Peers[i].GetStoreId(),
			})
		}
		// 2. modify original Region
		d.updateStoreMeta(d.peer, d.Region(), newRegion1)
		meta.WriteRegionState(wb, newRegion1, rspb.PeerState_Normal)
		meta.WriteRegionState(wb, newRegion2, rspb.PeerState_Normal)

		// 3. create NEW Peer with NEW region2, and register
		newPeer, err := createPeer(d.storeID(), d.ctx.cfg, d.ctx.regionTaskSender, d.ctx.engine, newRegion2)
		if err != nil {
			log.Panic(err)
		}
		d.ctx.router.register(newPeer)
		d.ctx.router.send(split.GetNewRegionId(), message.Msg{Type: message.MsgTypeStart, RegionID: split.GetNewRegionId()})
		d.updateStoreMeta(newPeer, nil, newRegion2)

		if d.IsLeader() {
			d.SendResponse(response, entry.GetIndex(), entry.GetTerm(), false)
			// the newly created Peer will be created by heartbeat from the leader
			d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		}
	default:
		log.Errorf("invalid cmd type")
	}
}

func (d *peerMsgHandler) SendResponse(response *raft_cmdpb.RaftCmdResponse, index, term uint64, isSnap bool) {
	for idx, proposal := range d.proposals {
		if proposal.index == index {
			if proposal.term == term {
				if isSnap {
					proposal.cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
				}
				proposal.cb.Done(response)
			} else {
				NotifyStaleReq(proposal.term, proposal.cb)
			}

			// 一旦某个 Index 处的 Entry 被 applied, 便可以认为之前所有的 Entry 均被 applied, 进行压缩防止过度增长
			if idx == len(d.proposals)-1 {
				d.proposals = d.proposals[:0]
			} else {
				d.proposals = d.proposals[idx+1:]
			}
			break
		}
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with key {%v}", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

// check correctness
func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID, leaderID := d.Region().GetId(), d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	if err := d.preProposeRaftCommand(msg); err != nil {
		// DPrintf("%s Done command {%+v} ERROR %s", d.Tag, msg, err)
		cb.Done(ErrResp(err))
		return
	}
	if msg.GetHeader().GetRegionId() != d.regionId {
		DPrintf("%s Done command {%+v} ERROR region id not found", d.Tag, msg)
		cb.Done(ErrRespRegionNotFound(msg.GetHeader().GetRegionId()))
		return
	}

	if len(msg.GetRequests()) != 0 {
		d.proposeNormalCommand(msg, cb)
	} else {
		d.proposeAdminCommand(msg, cb)
	}

	d.HandleRaftReady()
}

func (d *peerMsgHandler) proposeNormalCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	newProposal := &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	}

	data, err := msg.Marshal()
	if err != nil {
		cb.Done(ErrResp(err))
		return
	} else if err = d.RaftGroup.Propose(data); err != nil {
		cb.Done(ErrResp(err))
		return
	}

	d.proposals = append(d.proposals, newProposal)
	DPrintf("%s New Command {%+v} at Proposal %+v", d.Tag, msg, newProposal)
}

func (d *peerMsgHandler) proposeAdminCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	req := msg.GetAdminRequest()
	newProposal := &proposal{
		index: d.nextProposalIndex(),
		term:  d.Term(),
		cb:    cb,
	}

	switch req.GetCmdType() {
	case raft_cmdpb.AdminCmdType_ChangePeer:
		if d.RaftGroup.Raft.PendingConfIndex > d.RaftGroup.Raft.RaftLog.Applied() {
			return
		}

		// remove a leader of a 2-peers-group, do transfer leader first
		if req.GetChangePeer().GetChangeType() == eraftpb.ConfChangeType_RemoveNode && req.GetChangePeer().GetPeer().GetId() == d.PeerId() {
			p, maxm := uint64(0), uint64(0)
			for peer, prs := range d.RaftGroup.GetProgress() {
				if peer == d.PeerId() {
					continue
				}
				if prs.Match > maxm {
					maxm = prs.Match
					p = peer
				}
			}
			if p == 0 {
				log.Panic("no peer can become leader")
			}
			d.RaftGroup.TransferLeader(p)
		}
		newConfChange := eraftpb.ConfChange{
			ChangeType: req.GetChangePeer().GetChangeType(),
			NodeId:     req.GetChangePeer().GetPeer().GetId(),
		}
		newConfChange.Context, _ = msg.Marshal()
		d.RaftGroup.ProposeConfChange(newConfChange)
	case raft_cmdpb.AdminCmdType_CompactLog:
		data, err := msg.Marshal()
		if err != nil {
			cb.Done(ErrResp(err))
			return
		} else if err = d.RaftGroup.Propose(data); err != nil {
			cb.Done(ErrResp(err))
			return
		}
	case raft_cmdpb.AdminCmdType_TransferLeader:
		d.RaftGroup.TransferLeader(req.GetTransferLeader().GetPeer().GetId())
		cb.Done(&raft_cmdpb.RaftCmdResponse{
			Header: &raft_cmdpb.RaftResponseHeader{},
			AdminResponse: &raft_cmdpb.AdminResponse{
				CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
			},
		})
	case raft_cmdpb.AdminCmdType_Split:
		data, err := msg.Marshal()
		if err != nil {
			cb.Done(ErrResp(err))
			return
		} else if err = d.RaftGroup.Propose(data); err != nil {
			cb.Done(ErrResp(err))
			return
		}
	default:
		log.Error("invalid cmd type")
	}

	d.proposals = append(d.proposals, newProposal)
	DPrintf("%s New Command {%+v} at Proposal %+v", d.Tag, msg, newProposal)
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		DPrintf("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			DPrintf("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		DPrintf("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		DPrintf("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	DPrintf("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		DPrintf("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			DPrintf("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		DPrintf("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// destroy self, include Regions and regionRanges in storeMeta
func (d *peerMsgHandler) destroyPeer() {
	DPrintf("%s starts destroy, with Region {%+v}", d.Tag, d.Region())
	regionID := d.regionId

	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		DPrintf("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		DPrintf("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				DPrintf("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					DPrintf("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			DPrintf("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
