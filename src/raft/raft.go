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
	"encoding/json"
	"fmt"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
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

type LogEntry struct {
	Term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status      RaftStatus
	currentTerm int
	votedFor    int
	logs        []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	updateAt time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.status == RaftStatusLeader)
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
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
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
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
	// Your data here (2A, 2B).
	Term        int
	CandidateID int
	// LastLogIndex int
	// LastLogTerm  int
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
	defer func() {
		_args, _ := json.Marshal(args)
		_reply, _ := json.Marshal(reply)
		fmt.Printf("%v: args:%v, reply:%v, rf.me:%v, rf.status:%v, rf.currentTerm:%v, rf.votedFor:%v\n",
			time.Now().Format("2006-01-02 15:04:05.000"), string(_args), string(_reply), rf.me, rf.status, rf.currentTerm, rf.votedFor)
	}()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm || rf.status != RaftStatusFollower {
		return
	}
	// 抢票
	if args.Term == rf.currentTerm && rf.votedFor >= 0 && rf.votedFor != args.CandidateID {
		return
	}
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateID
	rf.updateAt = time.Now()
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	reply.Success = false
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.updateAt = time.Now()
		reply.Success = true
	}
	reply.Term = rf.currentTerm
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	start := time.Now()
	_args, _ := json.Marshal(args)
	fmt.Printf("%v: call sendRequestVote, req:%v, to_server:%v\n", start.Format("2006-01-02 15:04:05.000"), string(_args), server)

	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	end := time.Now()
	_reply, _ := json.Marshal(reply)
	fmt.Printf("%v: call sendRequestVote, resp:%v, start:%v, cost:%v\n", end.Format("2006-01-02 15:04:05.000"), string(_reply), start.Format("2006-01-02 15:04:05.000"), end.Sub(start))

	return ok
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
	isLeader := true

	// Your code here (2B).

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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) processFollower() {
	rf.mu.Lock()
	if rf.updateAt.Before(time.Now().Add(-RaftElectionTimeout)) {
		rf.currentTerm += 1
		rf.status = RaftStatusCandidate
		rf.votedFor = rf.me
	}
	rf.mu.Unlock()
}

func (rf *Raft) processCandidate() {
	count := 0
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		args := &RequestVoteArgs{
			Term:        rf.currentTerm,
			CandidateID: rf.me,
		}
		rf.mu.Unlock()

		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(i, args, reply)
		if !ok {
			continue
		}
		if !reply.VoteGranted {
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.status = RaftStatusFollower
				rf.votedFor = -1
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		} else {
			count += 1
		}
	}
	nagree := count + 1
	threshold := (len(rf.peers) + 1) >> 1
	fmt.Printf("%v: rf.me=%v, nagree=%v, threshold=%v\n", time.Now().Format("2006-01-02 15:04:05.000"), rf.me, nagree, threshold)

	rf.mu.Lock()
	if nagree >= threshold {
		rf.status = RaftStatusLeader
	} else {
		rf.status = RaftStatusFollower
		rf.votedFor = -1
		rf.updateAt = time.Now()
	}
	rf.mu.Unlock()
}

func (rf *Raft) processLeader() {
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		rf.mu.Lock()
		args := &AppendEntriesArgs{
			Term:     rf.currentTerm,
			LeaderID: rf.me,
		}
		rf.mu.Unlock()
		reply := &AppendEntriesReply{}
		ok := rf.sendAppendEntries(i, args, reply)
		if !ok {
			continue
		}
		rf.mu.Lock()
		if !reply.Success && reply.Term > rf.currentTerm {
			rf.status = RaftStatusFollower
		}
		rf.mu.Unlock()
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		fmt.Printf("%v: rf.me=%v, rf.status=%v, rf.currentTerm=%v\n", time.Now().Format("2006-01-02 15:04:05.000"), rf.me, rf.status, rf.currentTerm)
		switch rf.status {
		case RaftStatusFollower:
			time.Sleep(time.Duration(time.Millisecond * 150))
			if rf.status == RaftStatusFollower {
				rf.processFollower()
			} else {
				continue
			}
		case RaftStatusCandidate:
			rf.processCandidate()
		case RaftStatusLeader:
			rf.processLeader()
		}
		cost := (100 + rand.Intn(100))
		time.Sleep(time.Duration(cost) * time.Millisecond)
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
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.updateAt = time.Now()

	rf.status = RaftStatusFollower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
