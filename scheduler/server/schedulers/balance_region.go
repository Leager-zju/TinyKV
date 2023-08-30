// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// 1. First, the Scheduler will select all suitable stores.
	// 		Then sort them according to their region size.
	// 		Then the Scheduler tries to find regions to
	// 		move from the store with the biggest region size.
	stores := cluster.GetStores()
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})

	var regionToMove *core.RegionInfo
	var original int
	target := len(stores) - 1
	// 2.	First, it will try to select a pending region.
	// 		If there isn’t a pending region, it will try to find a follower region.
	// 		If it still cannot pick out one region, it will try to pick leader regions.
	// 		Finally, it will select out the region to move,
	// 		or the Scheduler will try the next store which has a smaller region size
	// 		until all stores will have been tried.
	for i, store := range stores {
		// In short, a suitable store should be up
		// and the down time cannot be longer than MaxStoreDownTime of the cluster
		// If the difference between the original and target stores’ region sizes is too small,
		// after we move the region from the original store to the target store,
		// the Scheduler may want to move back again next time.
		// So we have to make sure that the difference has to be bigger than
		// [two times the approximate size of the region], which ensures that after moving,
		// the target store’s region size is still smaller than the original store.
		if !store.IsUp() || store.DownTime() > cluster.GetMaxStoreDownTime() {
			continue
		}
		cluster.GetPendingRegionsWithLock(store.GetID(), func(container core.RegionsContainer) {
			regionToMove = container.RandomRegion([]byte{}, []byte{})
		})
		if regionToMove != nil && stores[i].GetRegionSize() > stores[target].GetRegionSize()+regionToMove.GetApproximateSize()*2 {
			original = i
			break
		}

		cluster.GetFollowersWithLock(store.GetID(), func(container core.RegionsContainer) {
			regionToMove = container.RandomRegion([]byte{}, []byte{})
		})
		if regionToMove != nil && stores[i].GetRegionSize() > stores[target].GetRegionSize()+regionToMove.GetApproximateSize()*2 {
			original = i
			break
		}

		cluster.GetLeadersWithLock(store.GetID(), func(container core.RegionsContainer) {
			regionToMove = container.RandomRegion([]byte{}, []byte{})
		})
		if regionToMove != nil && stores[i].GetRegionSize() > stores[target].GetRegionSize()+regionToMove.GetApproximateSize()*2 {
			original = i
			break
		}
	}

	if regionToMove == nil {
		log.Infof("No region to remove")
		return nil
	}

	for target >= 0 {
		ids := cluster.GetRegion(regionToMove.GetID()).GetStoreIds()
		if _, ok := ids[stores[target].GetID()]; !ok {
			break
		}
		target--
		// else: target store has the same region as regionToMove
	}

	// 3. After you pick up one region to move, the Scheduler will select a store as the target.
	// 		Actually, the Scheduler will select the store with the smallest region size.
	//		Then the Scheduler will judge whether this movement is valuable,
	//		by checking the difference between region sizes of the original store and the target store.
	//		If the difference is big enough, the Scheduler should allocate a new peer on the target store
	//		and create a move peer operator.
	newPeer, err := cluster.AllocPeer(stores[target].GetID())
	if err != nil {
		log.Panic(err)
	}
	op, err := operator.CreateMovePeerOperator(s.GetName(), cluster, regionToMove, operator.OpBalance, stores[original].GetID(), stores[target].GetID(), newPeer.GetId())
	if err != nil {
		log.Panic(err)
	}
	log.Infof("New Operator: %+v", op)
	return op
}
