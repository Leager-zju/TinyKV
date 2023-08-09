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
	"fmt"

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

	// Your Data Here (2A).
	truncatedIndex uint64

	lastAppend *pb.Entry
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	hardState, _, _ := storage.InitialState()
	firstIndex, _ := storage.FirstIndex() // truncatedIndex + 1
	truncatedIndex := firstIndex - 1
	truncatedTerm, _ := storage.Term(truncatedIndex)
	lastIndex, _ := storage.LastIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)

	newLog := &RaftLog{
		storage:         storage,
		committed:       hardState.Commit,
		stabled:         lastIndex,
		entries:         append([]pb.Entry{{Index: truncatedIndex, Term: truncatedTerm}}, entries...),
		pendingSnapshot: nil,
		truncatedIndex:  truncatedIndex,
	}
	newLog.updateLastAppend()
	return newLog
}

func (l *RaftLog) length() uint64 {
	return uint64(len(l.entries))
}

func (l *RaftLog) Index2idx(Index uint64) (idx uint64) {
	if Index < l.TruncatedIndex() {
		panic(fmt.Sprintf("Index %d UnderFlow", Index))
	}
	idx = Index - l.TruncatedIndex()
	return
}

func (l *RaftLog) LastAppend() *pb.Entry {
	return l.lastAppend
}

func (l *RaftLog) updateLastAppend() {
	l.lastAppend = &l.entries[l.length()-1]
}

func (l *RaftLog) appendEntry(e pb.Entry) {
	l.entries = append(l.entries, e)
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	if len(l.entries) == 1 { // only Truncated Entry
		return []pb.Entry{}
	}
	return l.entries[l.Index2idx(l.TruncatedIndex())+1:]
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
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	appliedBegin, committedBegin := l.Index2idx(l.applied)+1, l.Index2idx(l.committed)+1

	if appliedBegin >= l.length() { // all entries are applied, return empty
		return []pb.Entry{}
	}
	return l.entries[appliedBegin:committedBegin]
}

func (l *RaftLog) TruncatedIndex() uint64 {
	return l.truncatedIndex
}

// LastTerm return the last term of the log entries
func (l *RaftLog) LastTerm() uint64 {
	return l.LastAppend().GetTerm()
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	return l.LastAppend().GetIndex()
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (result uint64, err error) {
	return l.storage.Term(i)
}
