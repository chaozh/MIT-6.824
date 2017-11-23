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
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
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
	currentTerm      int
	votedFor         int
	logs             []LogEntry
	commitIndex      int
	lastApplied      int
	nextIndex        []int
	matchIndex       []int
	state            int
	leaderTimeout    int
	heartBeatTimeout int
	agreeCount       int
	applyCh          chan ApplyMsg
}

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2
)

const (
	Tick      = 10
	HeartBeat = 100
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
	isleader = rf.state == Leader
	return term, isleader
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
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	if data != nil && len(data) > 0 { // bootstrap without any state?
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.currentTerm)
		d.Decode(&rf.votedFor)
		d.Decode(&rf.logs)
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
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

	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	isNeedPersist := false
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		isNeedPersist = true
	}
	reply.Term = rf.currentTerm

	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateId
	isLogUpToDate := args.LastLogTerm > rf.lastLogTerm() || (args.LastLogTerm == rf.lastLogTerm() && args.LastLogIndex >= rf.lastLogIndex())
	if canVote && isLogUpToDate {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		isNeedPersist = true
	}

	if isNeedPersist {
		rf.persist()
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isNeedPersist := false
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		isNeedPersist = true
	}
	reply.Term = rf.currentTerm
	reply.Success = true

	if args.Term < rf.currentTerm {
		reply.Success = false
	} else if args.PrevLogIndex > rf.lastLogIndex() || (args.PrevLogIndex >= 0 && rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm) {
		reply.Success = false
		rf.resetElectionTimeout()
	} else {
		i := 0
		for index, log := range args.Entries {
			i = args.PrevLogIndex + index + 1
			if rf.lastLogIndex() >= i && rf.logs[i].Term != log.Term {
				rf.logs = rf.logs[:i]
			}
			rf.logs = append(rf.logs, log)
			isNeedPersist = true
		}
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit < rf.lastLogIndex() {
				rf.commitIndex = args.LeaderCommit
			} else {
				rf.commitIndex = rf.lastLogIndex()
			}
		}
		if rf.commitIndex > rf.lastApplied {
			go rf.applyLog()
		}
		reply.Success = true
		rf.resetElectionTimeout()
	}

	if isNeedPersist {
		rf.persist()
	}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		isLeader = false
	} else {
		rf.logs = append(rf.logs, LogEntry{Term: rf.currentTerm, Command: command})
		index = len(rf.logs)
		term = rf.currentTerm
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf.logs = make([]LogEntry, 0)
	rf.applyCh = applyCh
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.becomeFollower(0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startLoop()
	return rf
}

func (rf *Raft) startLoop() {
	for {
		time.Sleep(time.Millisecond * Tick)

		rf.mu.Lock()
		switch rf.state {
		case Leader:
			rf.heartBeatTimeout -= Tick
			if rf.heartBeatTimeout <= 0 {
				rf.resetHeartBeat()
				rf.sendAppendEntriesPeriod()
			}
		default:
			rf.leaderTimeout -= Tick
			if rf.leaderTimeout <= 0 {
				rf.becomeCandidate()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) resetElectionTimeout() {
	rf.leaderTimeout = 150 + rand.Intn(150)
}

func (rf *Raft) resetHeartBeat() {
	rf.heartBeatTimeout = HeartBeat
}

func (rf *Raft) becomeFollower(term int) {
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = term
	DPrintf("%v, become follower term %v", rf.me, rf.currentTerm)
	rf.resetElectionTimeout()
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.agreeCount = 1
	rf.resetElectionTimeout()
	rf.persist()
	DPrintf("%v, become candidate term %v", rf.me, rf.currentTerm)

	request := func(server int) {
		rf.mu.Lock()
		args := RequestVoteArgs{
			rf.currentTerm, rf.me,
			rf.lastLogIndex(), rf.lastLogTerm(),
		}
		rf.mu.Unlock()

		reply := RequestVoteReply{}
		ok := rf.sendRequestVote(server, &args, &reply)

		if !ok {
			return
		}

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.persist()
			return
		}
		DPrintf("%v, vote result term %v, state %v, grant from %v %v", rf.me, rf.currentTerm, rf.state, server, reply.VoteGranted)
		if rf.state == Candidate && reply.VoteGranted {
			rf.agreeCount += 1
			if rf.agreeCount >= majority(len(rf.peers)) {
				rf.becomeLeader()
			}
		}
	}

	for i := range rf.peers {
		if i != rf.me {
			go request(i)
		}
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	for i := range rf.peers {
		if i != rf.me {
			rf.nextIndex[i] = rf.lastLogIndex() + 1
			rf.matchIndex[i] = -1
		}
	}
	DPrintf("vote succ, %v become leader term %v", rf.me, rf.currentTerm)
	rf.resetHeartBeat()
	rf.sendAppendEntriesPeriod()
}

func (rf *Raft) sendAppendEntriesPeriod() {
	send := func(server int) {
		rf.mu.Lock()
		var args AppendEntriesArgs
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		args.PrevLogIndex = rf.nextIndex[server] - 1
		if args.PrevLogIndex >= 0 {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
			args.Entries = rf.logs[rf.nextIndex[server]:]
		} else {
			args.Entries = rf.logs
		}
		rf.mu.Unlock()

		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &args, &reply)

		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok || rf.state != Leader {
			return
		}

		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			rf.persist()
			return
		}

		if reply.Success {
			appendLogLen := len(args.Entries)
			if appendLogLen == 0 {
				return
			}
			rf.nextIndex[server] = args.PrevLogIndex + appendLogLen + 1
			rf.matchIndex[server] = args.PrevLogIndex + appendLogLen

			agreeCount := 1
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= rf.matchIndex[server] {
					agreeCount += 1
				}
			}

			if agreeCount >= majority(len(rf.peers)) && rf.logs[rf.matchIndex[server]].Term == rf.currentTerm {
				rf.commitIndex = rf.matchIndex[server]
			}

			if rf.commitIndex > rf.lastApplied {
				go rf.applyLog()
			}
		} else {
			rf.nextIndex[server] -= 1
		}
	}

	for i := range rf.peers {
		if i != rf.me {
			go send(i)
		}
	}
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{Index: i + 1, Command: rf.logs[i].Command}
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) lastLogTerm() int {
	if rf.lastLogIndex() < 0 {
		return 0
	} else {
		return rf.logs[rf.lastLogIndex()].Term
	}
}

func majority(serverNum int) int {
	return serverNum/2 + 1
}
