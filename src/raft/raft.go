package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/caitong93/MIT-6.824/src/labrpc"
	"golang.org/x/sync/errgroup"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Command interface{}
	Term    int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile state on all servers.
	commitIndex int
	lastApplied int
	role        int32

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	readyCh         chan struct{}
	appendEntriesCh chan Operation
	requestVoteCh   chan Operation
	operationsCh    chan Operation
	electionTimeout time.Duration
	resetElection   func()
	resetHeartbeat  func()
	ctx             context.Context
	cancel          func()
}

type Operation struct {
	call  func()
	errCh chan error
}

const (
	RaftRoleFollower int32 = iota
	RaftRoleCandidate
	RaftRoleLeader
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = rf.isLeader()

	return term, isleader
}

func (rf *Raft) isLeader() bool {
	return rf.role == RaftRoleLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	op := Operation{
		call: func() {
			rf.handleRequestVote(args, reply)
		},
		errCh: make(chan error),
	}

	rf.requestVoteCh <- op
	err := <-op.errCh
	if err != nil {
		fmt.Println("Err handle RequestVote:", err)
	}
}

func (rf *Raft) requestVoteLoop() {
	select {
	case <-rf.ctx.Done():
		return
	case <-rf.readyCh:
	}

	for {
		select {
		case <-rf.ctx.Done():
			return
		case op := <-rf.requestVoteCh:
			op.call()
			if op.errCh != nil {
				op.errCh <- nil
			}
		}
	}
}

func (rf *Raft) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Println("RequestVote node=", rf.me, "term=", rf.currentTerm, "role=", rf.role, "votedFor=", rf.votedFor, "args:", args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm)
	defer func() {
		fmt.Println("RequestVote done, node=", rf.me, "term=", rf.currentTerm, "votedFor=", rf.votedFor, "args:", args.CandidateID, args.Term, args.LastLogIndex, args.LastLogTerm)
	}()
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if rf.currentTerm == args.Term && rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		return
	}

	lastLogTerm := rf.logs[rf.commitIndex].Term
	if args.LastLogIndex < rf.commitIndex || args.LastLogTerm < lastLogTerm {
		return
	}

	if args.LastLogIndex == rf.commitIndex && args.LastLogTerm != lastLogTerm {
		return
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateID
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
	rf.operationsCh <- Operation{
		call: rf.resetElection,
	}
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	rf.cancel()
	// Your code here, if desired.
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	op := Operation{
		call: func() {
			rf.handleAppendEntries(args, reply)
		},
		errCh: make(chan error),
	}
	rf.appendEntriesCh <- op
	err := <-op.errCh
	if err != nil {
		fmt.Println("Err handle AppendEntries:", err)
	}
}

func (rf *Raft) appendEntriesLoop() {
	select {
	case <-rf.ctx.Done():
		return
	case <-rf.readyCh:
	}

	for {
		select {
		case <-rf.ctx.Done():
			return
		case op := <-rf.appendEntriesCh:
			op.call()
			if op.errCh != nil {
				op.errCh <- nil
			}
		}
	}
}

func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// fmt.Println("AppendEntries", args.LeaderID, args.Term, args.PrevLogIndex, args.PrevLogTerm, rf.currentTerm)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}

	if rf.role == RaftRoleLeader && rf.currentTerm == args.Term {
		// One term, one leader
		return
	}

	if args.PrevLogIndex >= len(rf.logs) || args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		return
	}

	// Become follower
	rf.becomeFollower(args.Term)

	// TODO: append logs
	return
}

func (rf *Raft) becomeFollower(term int) {
	if rf.role != RaftRoleFollower {
		fmt.Println("Node", rf.me, "become follower, term=", term)
		rf.role = RaftRoleFollower
	}
	rf.updateTerm(term)
	rf.operationsCh <- Operation{
		call: func() {
			rf.resetElection()
		},
	}
}

func (rf *Raft) updateTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
	}
}

func (rf *Raft) gatherVotes() bool {
	rf.mu.Lock()
	me, term, peerNum := rf.me, rf.currentTerm, len(rf.peers)

	if rf.role != RaftRoleCandidate {
		rf.mu.Unlock()
		return false
	}
	if rf.votedFor != -1 && rf.votedFor != rf.me {
		rf.mu.Unlock()
		return false
	}

	fmt.Println("Gather votes,", "node=", rf.me, "term=", rf.currentTerm, "votedFor=", rf.votedFor, "role=", rf.role)

	resultCh := make(chan bool, len(rf.peers)-1)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		node := i
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateID:  rf.me,
			LastLogIndex: rf.commitIndex,
			LastLogTerm:  rf.logs[rf.commitIndex].Term,
		}
		go func() {
			// fmt.Println("ReqeustVote from", me, "to", node, "start")
			reply := &RequestVoteReply{}
			rf.sendRequestVote(node, args, reply)
			resultCh <- reply.VoteGranted
			// fmt.Println("ReqeustVote from", me, "to", node, "end")
		}()
	}
	rf.mu.Unlock()

	grantedVotes := 1
	defer func() {
		fmt.Println("Got", grantedVotes, "votes,", "node=", me, "term=", term)
	}()
	for ok := range resultCh {
		if ok {
			grantedVotes++
			if grantedVotes*2 > peerNum {
				rf.mu.Lock()
				rf.becomeLeader()
				rf.mu.Unlock()
				return true
			}
		}
	}
	return false
}

func (rf *Raft) becomeLeader() {
	fmt.Println("Node", rf.me, "become leader")
	rf.role = RaftRoleLeader
	rf.operationsCh <- Operation{
		call: func() {
			rf.resetHeartbeat()
			rf.resetElection()
		},
	}
}

func (rf *Raft) sendHeartBeats() {
	rf.mu.Lock()
	if rf.role != RaftRoleLeader {
		rf.mu.Unlock()
		return
	}
	var g errgroup.Group
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		node := i
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: rf.matchIndex[node],
			PrevLogTerm:  rf.logs[rf.matchIndex[node]].Term,
			LeaderCommit: rf.commitIndex,
		}
		g.Go(func() error {
			// fmt.Println("Send heartbeat to peer", i, "node=", rf.me)
			reply := &AppendEntriesReply{}
			rf.sendAppendEntries(node, args, reply)
			if reply.Success {
				// fmt.Println("Peer", i, "is OK", "node=", rf.me)
			}
			return nil
		})
	}
	rf.mu.Unlock()
	g.Wait()
}

func (rf *Raft) loop() {
	var (
		electionTimeoutCh <-chan time.Time
		heartBeatCh       <-chan time.Time
	)

	rf.resetElection = func() {
		var role int32
		rf.mu.Lock()
		role = rf.role
		rf.mu.Unlock()
		if role == RaftRoleLeader {
			electionTimeoutCh = nil
		} else {
			rf.electionTimeout = randElectionTimeout()
			electionTimeoutCh = time.After(rf.electionTimeout)
		}
	}
	rf.resetHeartbeat = func() {
		var role int32
		rf.mu.Lock()
		role = rf.role
		rf.mu.Unlock()
		if role != RaftRoleLeader {
			heartBeatCh = nil
		} else {
			if heartBeatCh == nil {
				heartBeatCh = time.After(0 * time.Millisecond)
			} else {
				heartBeatCh = time.After(50 * time.Millisecond)
			}
		}
	}
	rf.resetElection()
	close(rf.readyCh)

	fmt.Println("Node", rf.me, ": start maintain.")

	for {
		select {
		case <-rf.ctx.Done():
			return
		case op := <-rf.operationsCh:
			op.call()
			if op.errCh != nil {
				op.errCh <- nil
			}
		case <-electionTimeoutCh:
			rf.resetElection()
			rf.mu.Lock()
			if rf.role == RaftRoleFollower {
				// Become cadidate and vote for self
				rf.role = RaftRoleCandidate
				rf.votedFor = rf.me
			}
			if rf.role == RaftRoleCandidate {
				rf.currentTerm++
				go rf.gatherVotes()
			}
			rf.mu.Unlock()
		case <-heartBeatCh:
			rf.resetHeartbeat()
			go rf.sendHeartBeats()
		}
	}
}

func randElectionTimeout() time.Duration {
	return durationRange(150*time.Millisecond, 200*time.Millisecond)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
// Notes(Tong): test multiple times to find every possible bug, go test -race -failfast  -count 50   -run TestInitialElection2A
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = RaftRoleFollower
	rf.electionTimeout = randElectionTimeout()
	rf.votedFor = -1
	rf.logs = append(rf.logs, LogEntry{})
	rf.nextIndex = make([]int, len(rf.peers), len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers), len(rf.peers))
	rf.operationsCh = make(chan Operation, 100)
	rf.appendEntriesCh = make(chan Operation, 100)
	rf.requestVoteCh = make(chan Operation, 100)
	rf.readyCh = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	rf.ctx = ctx
	rf.cancel = cancel

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.loop()
	go rf.requestVoteLoop()
	go rf.appendEntriesLoop()

	return rf
}
