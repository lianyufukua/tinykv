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
	"errors"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raft := &Raft{
		id:               c.ID,
		//Term:             0,
		//Vote:             0,
		RaftLog:          newLog(c.Storage),
		Prs:              make(map[uint64]*Progress),
		//State:            0,
		votes:            make(map[uint64]bool),
		//msgs:             nil,
		//Lead:             0,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		//heartbeatElapsed: 0,
		//electionElapsed:  0,
		//leadTransferee:   0,
		//PendingConfIndex: 0,
	}
	lastIndex := raft.RaftLog.LastIndex()
	firstIndex, _ := raft.RaftLog.storage.FirstIndex()
	state, confSt, _ := raft.RaftLog.storage.InitialState()
	var peers []uint64
	if c.peers == nil {
		peers = confSt.Nodes
	} else {
		peers = c.peers
	}
	
	for _, peer := range peers {
		if peer == raft.id {
			raft.Prs[peer] = &Progress{Next: lastIndex + 1, Match: lastIndex}
		} else {
			raft.Prs[peer] = &Progress{Next: lastIndex + 1, Match: firstIndex - 1}
		}
	}
	raft.becomeFollower(0, None)
	raft.randomElectionTimeout = raft.electionTimeout + rand.Intn(raft.electionTimeout)
	raft.Vote, raft.Term, raft.RaftLog.committed = state.GetVote(), state.GetTerm(), state.GetCommit()
	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	lastindex := r.Prs[to].Next - 1
	lastlogterm, err := r.RaftLog.Term(lastindex)
	if err != nil {
		panic(err)
	}
	i := r.RaftLog.SliceEntries(r.Prs[to].Next)
	ents := make([]*pb.Entry, 0)
	n := len(r.RaftLog.entries)
	for ; i < n; i++ {
		ents = append(ents, &r.RaftLog.entries[i])
	}
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastlogterm,
		Index:   lastindex,
		Entries: ents,
		Commit:  r.RaftLog.committed,
	}
	r.msgs = append(r.msgs, msg)
	return true
}

func (r *Raft) sendAppendResponse(to uint64, reject bool, term, index uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: term,
		Index:   index,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVote(to uint64) {
	lastIndex := r.RaftLog.LastIndex()
	lastLogTerm, _ := r.RaftLog.Term(lastIndex)
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: lastLogTerm,
		Index:   lastIndex,
	}
	r.msgs = append(r.msgs, msg)
}

func (r *Raft) sendRequestVoteResponse(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateCandidate:
		r.electionElapsed++
		if r.electionElapsed >= r.randomElectionTimeout {
			r.electionElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgHup})
		}
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.Lead = None
	r.Term++
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.heartbeatElapsed = 0
	lastIndex := r.RaftLog.LastIndex()
	for prs := range r.Prs {
		if prs == r.id {
			r.Prs[prs] = &Progress{Next: lastIndex + 2, Match: lastIndex + 1}
		} else {
			r.Prs[prs].Next = lastIndex + 1
		}
	}
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Term: r.Term, Index: r.RaftLog.LastIndex() + 1})
	for prs := range r.Prs {
		if prs == r.id {
			continue
		}
		r.sendAppend(prs)
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
	}
	switch r.State {
	case StateFollower:
		r.stepFollower(m)
	case StateCandidate:
		r.stepCandidate(m)
	case StateLeader:
		r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.heartbeatElapsed = 0
		r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		if len(r.Prs) == 1 {
			r.becomeLeader()
			return
		}
		for prs := range r.Prs {
			if prs == r.id {
				continue
			}
			r.sendRequestVote(prs)
		}
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
}

func (r *Raft) stepCandidate(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.heartbeatElapsed = 0
		r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
		if len(r.Prs) == 1 {
			r.becomeLeader()
			return
		}
		for prs := range r.Prs {
			if prs == r.id {
				continue
			}
			r.sendRequestVote(prs)
		}
	case pb.MessageType_MsgAppend:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleRequestVoteResponse(m)
	case pb.MessageType_MsgSnapshot:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	}
}

func (r *Raft) stepLeader(m pb.Message) {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for prs := range r.Prs {
			if prs == r.id {
				continue
			}
			r.sendHeartbeat(prs)
		}
	case pb.MessageType_MsgPropose:
		r.appendEntries(m.Entries)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendEntriesResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleRequestVote(m)
	case pb.MessageType_MsgSnapshot:
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.sendAppend(m.From)
	}
}

func (r *Raft) appendEntries(entries []*pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i, entry := range entries {
		entry.Index = lastIndex + 1 + uint64(i)
		entry.Term = r.Term
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1
	for prs := range r.Prs {
		if prs == r.id {
			continue
		}
		r.sendAppend(prs)
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendAppendResponse(m.From, true, None, None)
		return
	}
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.Lead = m.From
	lastIndex := r.RaftLog.LastIndex()
	logTerm, err := r.RaftLog.Term(m.Index)
	if err != nil {
		panic(err)
	}
	if m.Index > lastIndex {
		r.sendAppendResponse(m.From, true, None, lastIndex+1)
		return
	}
	if logTerm != m.LogTerm {
		index := r.RaftLog.toEntryIndex(sort.Search(r.RaftLog.SliceEntries(m.Index+1),
			func(i int) bool { return r.RaftLog.entries[i].Term == logTerm }))
		r.sendAppendResponse(m.From, true, logTerm, index)
		return
	}
	for i, entry := range m.Entries {
		if entry.Index > r.RaftLog.LastIndex() {
			n := len(m.Entries)
			for j := i; j < n; j++ {
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[j])
			}
			break
		} else {
			term, err := r.RaftLog.Term(entry.Index)
			if err != nil {
				panic(err)
			}
			if term != entry.Term {
				index := r.RaftLog.SliceEntries(entry.Index)
				r.RaftLog.entries[index] = *entry
				r.RaftLog.entries = r.RaftLog.entries[:index+1]
				if r.RaftLog.stabled >= entry.Index {
					r.RaftLog.stabled = entry.Index - 1
				}
			}
		}
	}
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.sendAppendResponse(m.From, false, None, r.RaftLog.LastIndex())
}

func (r *Raft) handleAppendEntriesResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	if m.Reject {
		index := m.Index
		logTerm := m.LogTerm
		sliceIndex := sort.Search(len(r.RaftLog.entries),
			func(i int) bool { return r.RaftLog.entries[i].Term > logTerm })
		if sliceIndex > 0 && r.RaftLog.entries[sliceIndex-1].Term == logTerm {
			index = r.RaftLog.toEntryIndex(sliceIndex)
		}
		r.Prs[m.From].Next = index
		r.sendAppend(m.From)
		return
	}
	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1
	r.UpdateCommit()
	return
}

func (r *Raft) UpdateCommit() {
	match := make(uint64Slice, len(r.Prs))
	i := 0
	for _, prs := range r.Prs {
		match[i] = prs.Match
		i++
	}
	sort.Sort(match)
	n := match[(len(r.Prs)-1)/2]
	if n > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(n)
		if err != nil {
			panic(err)
		}
		if logTerm == r.Term {
			r.RaftLog.committed = n
			for prs := range r.Prs {
				if prs == r.id {
					continue
				}
				r.sendAppend(prs)
			}
		}
	}
}

func (r *Raft) handleRequestVote(m pb.Message) {
	if m.Term < r.Term {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	if r.Vote != None && r.Vote != m.From {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	lastIndex := r.RaftLog.LastIndex()
	logt, _ := r.RaftLog.Term(lastIndex)
	if logt > m.LogTerm {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	if logt == m.LogTerm && lastIndex > m.Index {
		r.sendRequestVoteResponse(m.From, true)
		return
	}
	r.Vote = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendRequestVoteResponse(m.From, false)
}

func (r *Raft) handleRequestVoteResponse(m pb.Message) {
	if m.Term < r.Term {
		return
	}
	r.votes[m.From] = !m.Reject
	grant := 0
	mid := len(r.Prs) / 2
	num := len(r.votes)
	for _, g := range r.votes {
		if g {
			grant++
		}
	}
	if grant > mid {
		r.becomeLeader()
	} else if num - grant > mid {
		r.becomeFollower(r.Term, None)
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.Lead = m.From
	r.electionElapsed = 0
	r.randomElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.sendHeartbeatResponse(m.From, false)
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		Lead: r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
