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
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Role int32

const (
	FollowerRole = iota
	CandidateRole
	LeaderRole
)

const ( //ms
	electionTimeout   = 150
	heartbeatInterval = 50
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role    Role
	applyCh chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	logs        LogEntries

	commitIndex int
	lastApplied int
	updatetime  bool

	// Leaderstate
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	isleader := rf.role == LeaderRole
	defer rf.mu.Unlock()
	DPrintf("[%d] GetState: %d, %v", rf.me, rf.currentTerm, isleader)
	return rf.currentTerm, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	DPrintf("[%d] persist", rf.me)
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs LogEntries
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		DPrintf("[%d] readPersist: decode error", rf.me)
		return
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.persist()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your code here (2A, 2B).
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
	// Your code here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d: RequestVote(Term:%d, CandidateID:%d, LIndex:%d,LTerm:%d)\n", rf.me, args.Term, args.CandidateID, args.LastLogIndex, args.LastLogTerm)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// Rule 1
	if args.Term < rf.currentTerm {
		return
	}
	// All servers Rule 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FollowerRole
		rf.votedFor = -1
		rf.persist()
	}
	rf.updatetime = true
	// Rule 2
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		currentLogIndex := rf.logs.GetLastIndex()
		currentLogTerm := rf.logs.GetLastLog().Term
		if !(args.LastLogTerm > currentLogTerm || (args.LastLogTerm == currentLogTerm && args.LastLogIndex >= currentLogIndex)) {
			DPrintf("%d: RequestVote(Term:%d, CandidateID:%d, LIndex:%d,LTerm:%d) granted\n", rf.me, args.Term, args.CandidateID, args.LastLogIndex, args.LastLogTerm)
			return
		}
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	}
	DPrintf("%d: RequestVote(Term:%d, CandidateID:%d, LIndex:%d,LTerm:%d) granted\n", rf.me, args.Term, args.CandidateID, args.LastLogIndex, args.LastLogTerm)
	rf.persist()
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesRes) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesRes struct {
	Term    int
	Success bool
	XIndex  int
	XTerm   int
	XLen    int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesRes) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	reply.Term = rf.currentTerm
	//Rule 1
	if args.Term < rf.currentTerm {
		return
	}
	//All Server Rule 2
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FollowerRole
		rf.votedFor = -1
		rf.persist()
	}
	//Candidate Rule 3
	if rf.role == CandidateRole {
		rf.role = FollowerRole
		rf.persist()
	}
	rf.updatetime = true
	//Rule 2
	if rf.logs.GetLastIndex() < args.PrevLogIndex {
		reply.XIndex = -1
		reply.XTerm = -1
		reply.XLen = rf.logs.Len()
		return
	}

	if rf.logs.Get(args.PrevLogIndex).Term != args.PrevLogTerm {
		// Fast Backup
		xTerm := rf.logs.Get(args.PrevLogIndex).Term
		for xIndex := args.PrevLogIndex - 1; xIndex >= 0; xIndex-- {
			if rf.logs.Get(xIndex).Term != xTerm {
				reply.XIndex = xIndex + 1
				reply.XTerm = xTerm
				reply.XLen = rf.logs.Len()
				return
			}
		}
	}
	if len(args.Entries) > 0 {
		for idx, value := range args.Entries {
			//Rule 4
			if idx+args.PrevLogIndex+1 >= rf.logs.Len() {
				rf.logs.Append(args.Entries[idx:]...)
				break
			} else if rf.logs.Get(idx+args.PrevLogIndex+1).Term != value.Term { //Rule 3
				rf.logs.Truncate(idx + args.PrevLogIndex + 1)
				rf.logs.Append(args.Entries[idx:]...)
				break
			}
		}
		rf.persist()
	}
	//Rule 5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, rf.logs.GetLastIndex())
		rf.apply()
	}
	reply.Success = true
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.role == LeaderRole
	term = rf.currentTerm
	if !isLeader {
		return index, term, isLeader
	}

	entry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs.Append(entry)
	index = rf.logs.GetLastIndex()
	term = rf.currentTerm
	rf.persist()
	DPrintf("[%d]: Start(%v) index:%d, term:%d\n", rf.me, command, index, term)
	rf.startAppend(false)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("%d: kill", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	DPrintf("%d: ticker start", rf.me)
	t := time.Now()
	electionTime := t.Add(time.Duration(rand.Intn(electionTimeout)) * time.Millisecond)
	for !rf.killed() {
		time.Sleep(time.Millisecond * time.Duration(heartbeatInterval))
		rf.mu.Lock()
		if rf.role == LeaderRole {
			rf.startAppend(true)
		} else if rf.updatetime {
			rf.updatetime = false
			t = time.Now()
			electionTime = t.Add(time.Duration(rand.Intn(electionTimeout)) * time.Millisecond)
		} else if time.Now().After(electionTime) {
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

//return a random timeout
func (rf *Raft) randomTimeout(timeout int) int {
	return rand.Int()%timeout + timeout
}

func (rf *Raft) startElection() {
	me := rf.me
	rf.role = CandidateRole
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.updatetime = true
	voteCount := int32(len(rf.peers)/2 + 1)
	DPrintf("%d: start a new election, term %d, voteCount %d\n", rf.me, rf.currentTerm, voteCount)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: rf.logs.GetLastIndex(),
	}
	if rf.logs.Len() > 0 {
		args.LastLogTerm = rf.logs.GetLastLog().Term
	}
	for i := 0; i < len(rf.peers); i++ {
		if i == me {
			atomic.AddInt32(&voteCount, -1)
			continue
		}
		go rf.sendElection(i, args, &voteCount)
	}
	//DPrintf("%d: election finished, term %d,voteCount:%d\n", me, rf.currentTerm, voteCount)
}

func (rf *Raft) sendElection(i int, args RequestVoteArgs, voteCount *int32) {
	reply := RequestVoteReply{}
	if ok := rf.sendRequestVote(i, &args, &reply); !ok {
		DPrintf("%d: send RequestVote to %d failed", rf.me, i)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("term:%d %d: send RequestVote to %d,%v", rf.currentTerm, rf.me, i, reply.VoteGranted)
	if !reply.VoteGranted {
		if reply.Term > rf.currentTerm {
			rf.role = FollowerRole
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
		}
		return
	}
	atomic.AddInt32(voteCount, -1)
	if atomic.LoadInt32(voteCount) == 0 && rf.role == CandidateRole {
		rf.role = LeaderRole
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = rf.logs.Len()
			rf.matchIndex[i] = 0
		}
		DPrintf("%d: become leader, term %d\n", rf.me, rf.currentTerm)
		rf.startAppend(true)
	}
}

func (rf *Raft) startAppend(heartbeat bool) {
	if rf.role != LeaderRole {
		return
	}
	count := int32(len(rf.peers)/2 + 1)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			atomic.AddInt32(&count, -1)
			continue
		}
		if rf.matchIndex[i] <= rf.logs.GetLastIndex() || heartbeat {
			nextIndex := rf.nextIndex[i]
			if nextIndex <= 0 {
				nextIndex = 1
			}
			if rf.logs.Len() < nextIndex {
				nextIndex = rf.logs.Len()
			}
			DPrintf("%d: send AppendEntries to %d, nextIndex %d, \n", rf.me, i, nextIndex)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: nextIndex - 1,
				PrevLogTerm:  rf.logs.Get(nextIndex - 1).Term,
				Entries:      rf.logs.Slice(nextIndex),
				LeaderCommit: rf.commitIndex,
			}
			go rf.sendEntries(i, args, &count)
		}
	}
}

func (rf *Raft) sendEntries(i int, args AppendEntriesArgs, count *int32) {
	reply := AppendEntriesRes{}
	if ok := rf.sendAppendEntries(i, &args, &reply); !ok {
		//DPrintf("%d: send heartbeat to %d failed", rf.me, i)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d: receive heartbeat reply from %d,%v,%v", rf.me, i, args, reply)
	if reply.Term > rf.currentTerm {
		DPrintf("%d: heartbeat to %d failed, reply term %d > current term %d", rf.me, i, reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.role = FollowerRole
		rf.votedFor = -1
		DPrintf("%d: become follower, term %d", rf.me, rf.currentTerm)
		rf.persist()
		return
	}
	if reply.Success {
		DPrintf("%d: heartbeat to %d success", rf.me, i)
		rf.matchIndex[i] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[i] = rf.matchIndex[i] + 1
		atomic.AddInt32(count, -1)
		if len(args.Entries) > 0 && atomic.LoadInt32(count) == 0 {
			//Figure 8
			if args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
				rf.commitIndex = args.PrevLogIndex + len(args.Entries)
				DPrintf("%d: leader commitIndex %d", rf.me, rf.commitIndex)
				rf.apply()
			}
		}
		return
	}
	if reply.XTerm == -1 {
		rf.nextIndex[i] = reply.XLen
	} else {
		lastLogInXTerm := rf.findLastLogIndexWithTerm(reply.XTerm)
		DPrintf("%v: lastLogInXTerm %v", rf.me, lastLogInXTerm)
		if lastLogInXTerm > 0 {
			rf.nextIndex[i] = lastLogInXTerm
		} else {
			rf.nextIndex[i] = reply.XIndex
		}
	}
	DPrintf("%d: heartbeat to %d failed, nextIndex %d", rf.me, i, rf.nextIndex[i])
}

func (rf *Raft) findLastLogIndexWithTerm(term int) int {
	for i := rf.logs.GetLastIndex(); i > 0; i-- {
		if rf.logs.Get(i).Term == term {
			return i
		} else if rf.logs.Get(i).Term < term {
			break
		}
	}
	return -1
}

func (rf *Raft) apply() {
	for ; rf.lastApplied <= rf.commitIndex; rf.lastApplied++ {
		if rf.lastApplied == 0 {
			continue
		}
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logs.Get(rf.lastApplied).Command,
			CommandIndex: rf.lastApplied,
		}
		DPrintf("%d: apply %d,%v", rf.me, rf.lastApplied, msg)
		rf.applyCh <- msg
	}
}
func (rf *Raft) applyer() {
	for !rf.killed() {
		select {
		default:
			time.Sleep(time.Millisecond * 100)
		}
	}
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	DPrintf("%d: init", rf.me)
	rf.logs = newLogEntries()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.role = FollowerRole
	rf.updatetime = false
	rf.votedFor = -1
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyer()

	return rf
}
