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
	"time"

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
	maxReplicas := cluster.GetMaxReplicas()
	// First, the Scheduler will select all suitable stores.
	stores := make([]*core.StoreInfo, 0)
	for _, store := range cluster.GetStores() {
		if isSuitable(store, cluster.GetMaxStoreDownTime()) {
			stores = append(stores, store)
		}
	}
	// Then sort them according to their region size.
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})

	// Then the Scheduler tries to find regions to
	// move from the store with the biggest region size.
	var originalRegion *core.RegionInfo
	original := len(stores)
	// First, it will try to select a pending region.
	// If there isn’t a pending region, it will try to find a follower region.
	// If it still cannot pick out one region, it will try to pick leader regions.
	// Finally, it will select out the region to move,
	// or the Scheduler will try the next store which has a smaller region size
	// until all stores will have been tried.
	cb := func(container core.RegionsContainer) {
		originalRegion = container.RandomRegion([]byte{}, []byte{})
	}
	// try get original region
	for i, store := range stores {
		cluster.GetPendingRegionsWithLock(store.GetID(), cb)
		if originalRegion != nil && len(originalRegion.GetPeers()) >= maxReplicas {
			original = i
			break
		}

		cluster.GetFollowersWithLock(store.GetID(), cb)
		if originalRegion != nil && len(originalRegion.GetPeers()) >= maxReplicas {
			original = i
			break
		}

		cluster.GetLeadersWithLock(store.GetID(), cb)
		if originalRegion != nil && len(originalRegion.GetPeers()) >= maxReplicas {
			original = i
			break
		}
	}
	if original == len(stores) {
		// log.Errorf("No region to remove")
		return nil
	}

	// After you pick up one region to move, the Scheduler will select a store as the target.
	// Actually, the Scheduler will select the store with the smallest region size.
	// Then the Scheduler will judge whether this movement is valuable,
	// by checking the difference between region sizes of the original store and the target store.
	target := len(stores) - 1
	for target >= 0 {
		ids := cluster.GetRegion(originalRegion.GetID()).GetStoreIds()
		_, regionInTargetStore := ids[stores[target].GetID()]
		if !regionInTargetStore && isDifferenceBigEnough(stores[original], stores[target], originalRegion) {
			break
		}
		target--
	}
	if target < 0 {
		return nil
	}

	// If the difference is big enough,
	// the Scheduler should allocate a new peer on the target store
	// and create a move peer operator.
	newPeer, err := cluster.AllocPeer(stores[target].GetID())
	if err != nil {
		log.Panic(err)
	}
	op, err := operator.CreateMovePeerOperator(s.GetName(), cluster, originalRegion, operator.OpBalance, stores[original].GetID(), stores[target].GetID(), newPeer.GetId())
	if err != nil {
		log.Panic(err)
	}
	log.Infof("New Operator: %+v", op)
	return op
}

func isSuitable(store *core.StoreInfo, maxStoreDownTime time.Duration) bool {
	// A suitable store should be [up]
	// and the [down time] <= [MaxStoreDownTime] of the cluster
	return store.IsUp() && store.DownTime() <= maxStoreDownTime
}

func isDifferenceBigEnough(originStore, targetStore *core.StoreInfo, originalRegion *core.RegionInfo) bool {
	// We have to make sure that [the difference between the original and target stores’ region sizes]
	// has to be bigger than [two times the approximate size of the region]
	return originStore.GetRegionSize() > targetStore.GetRegionSize()+originalRegion.GetApproximateSize()*2
}
