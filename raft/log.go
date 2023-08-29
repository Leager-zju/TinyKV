// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"github.com/pingcap-incubator/tinykv/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	hardState, _, err := storage.InitialState()
	if err != nil {
		log.Panic(err)
	}

	firstIndex, err := storage.FirstIndex() // truncatedIndex + 1
	if err != nil {
		log.Panic(err)
	}

	truncatedIndex := firstIndex - 1
	truncatedTerm, err := storage.Term(truncatedIndex)
	if err != nil {
		log.Panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		log.Panic(err)
	}

	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		log.Panic(err)
	}

	newLog := &RaftLog{
		storage:         storage,
		committed:       hardState.Commit,
		applied:         truncatedIndex,
		stabled:         lastIndex,
		entries:         append([]pb.Entry{{Index: truncatedIndex, Term: truncatedTerm}}, entries...),
		pendingSnapshot: nil,
	}
	return newLog
}

func (l *RaftLog) length() uint64 {
	return uint64(len(l.entries))
}

func (l *RaftLog) At(Index uint64) *pb.Entry {
	return &l.entries[l.Index2idx(Index)]
}

func (l *RaftLog) Index2idx(Index uint64) uint64 {
	if Index < l.TruncatedIndex() {
		log.Panicf("Index %d Under Flow, truncatedIndex %d", Index, l.TruncatedIndex())
	}
	if Index > l.LastIndex() {
		log.Panicf("Index %d Over Flow, lastIndex %d", Index, l.LastIndex())
	}
	return Index - l.TruncatedIndex()
}

func (l *RaftLog) LastAppend() *pb.Entry {
	if l.length() == 0 {
		log.Panic("raft log length can not be 0")
	}
	return &l.entries[l.length()-1]
}

func (l *RaftLog) FirstEntryWithTerm(term uint64) *pb.Entry {
	left, right := None, l.length()-1
	for left < right-1 {
		mid := left + (right-left)/2
		if mid > l.length() {
			log.Panicf("index [%d %d %d] out of range %d", left, mid, right, l.length()-1)
		}
		ent := &l.entries[mid]
		if ent.GetTerm() == term {
			right = mid
		} else if ent.GetTerm() > term {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	if left < l.length() && l.entries[left].GetTerm() == term {
		return &l.entries[left]
	}
	if right < l.length() && l.entries[right].GetTerm() == term {
		return &l.entries[right]
	}
	return nil
}

func (l *RaftLog) LastEntryWithTerm(term uint64) *pb.Entry {
	left, right := None, l.length()-1
	for left+1 < right {
		mid := left + (right-left)/2
		if mid > l.length() {
			log.Panicf("index [%d %d %d] out of range %d", left, mid, right, l.length()-1)
		}
		ent := &l.entries[mid]
		if ent.GetTerm() == term {
			left = mid
		} else if ent.GetTerm() < term {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}

	if right < l.length() && l.entries[right].GetTerm() == term {
		return &l.entries[right]
	}
	if left < l.length() && l.entries[left].GetTerm() == term {
		return &l.entries[left]
	}
	return nil
}

func (l *RaftLog) appendEntry(e pb.Entry) {
	l.entries = append(l.entries, e)
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	firstIndex, err := l.storage.FirstIndex() // change at every CompactLogRequest
	if err != nil {
		log.Panic(err)
	}
	truncatedIndex := firstIndex - 1
	truncatedTerm, err := l.storage.Term(truncatedIndex)
	if err != nil {
		log.Panic(err)
	}

	if l.TruncatedIndex() < truncatedIndex {
		if truncatedIndex <= l.LastAppend().GetIndex() {
			l.entries = l.entries[l.Index2idx(truncatedIndex):]
		} else {
			l.entries = []pb.Entry{{Term: truncatedTerm, Index: truncatedIndex}}
		}
	}
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	if len(l.entries) == 1 { // only Truncated Entry
		return []pb.Entry{}
	}
	return l.entries[1:]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	unstabledBegin := l.Index2idx(l.stabled) + 1
	if unstabledBegin >= l.length() {
		return []pb.Entry{}
	}
	return l.entries[unstabledBegin:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() []pb.Entry {
	appliedBegin := l.Index2idx(l.applied) + 1
	committedBegin := l.Index2idx(l.committed) + 1

	if appliedBegin >= l.length() { // all entries are applied, return empty
		return []pb.Entry{}
	}

	return l.entries[appliedBegin:committedBegin]
}

func (l *RaftLog) TruncatedIndex() uint64 {
	return l.entries[0].GetIndex()
}

func (l *RaftLog) TruncatedTerm() uint64 {
	return l.entries[0].GetTerm()
}

// LastTerm return the last term of the log entries
func (l *RaftLog) LastTerm() uint64 {
	return l.LastAppend().GetTerm()
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	return l.LastAppend().GetIndex()
}

// Applied return the last index of the applied log entries
func (l *RaftLog) Applied() uint64 {
	return l.applied
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (result uint64, err error) {
	if i > l.LastIndex() || i < l.TruncatedIndex() {
		return 0, pb.ErrIntOverflowEraftpb
	}
	return l.entries[l.Index2idx(i)].GetTerm(), nil
}
