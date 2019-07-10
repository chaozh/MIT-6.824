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
	"labrpc"
	"log"
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

type RaftStatus int

const (
	RaftStatus_Follower  RaftStatus = iota
	RaftStatus_Candidate RaftStatus = iota
	RaftStatus_Leader    RaftStatus = iota
)

type RaftLog struct {
	Term int
	Cmd  string
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	self        string
	currentTerm int
	votedFor    int
	log         []RaftLog

	commitIndex int
	lastApplied int

	status     RaftStatus
	nextIndex  map[int]int
	matchIndex map[int]int

	heartbeatDuration       time.Duration
	electionTimeoutDuration time.Duration

	lastHeartbeatTime time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	if rf.status == RaftStatus_Leader {
		return term, true
	}
	return term, false
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
	// Your data here (2A, 2B).
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
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.setLastHeartbeatTime(time.Now())
	term := rf.getTerm()
	reply.Term = term
	reply.VoteGranted = false
	if args.Term < term {
		return
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		thisLastLogIndex := len(rf.log) - 1
		if args.LastLogIndex < thisLastLogIndex {
			return
		} else if args.LastLogIndex == thisLastLogIndex {
			thisLastLog := rf.log[thisLastLogIndex]
			if args.LastLogTerm < thisLastLog.Term {
				return
			}
		}

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
		return
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
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftLog
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.getTerm()
	reply.Success = true
	if len(args.Entries) == 0 {
		//heartbeat设置为当前时间
		rf.setLastHeartbeatTime(time.Now())
		return
	}
	return
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

//处理只需要执行一次的任务，封装复杂性

type initOption struct {
	Peers                   []*labrpc.ClientEnd
	Me                      int
	Persister               *Persister
	HeartbeatDuration       time.Duration
	ElectionTimeoutDuration time.Duration
}

func (rf *Raft) init(op *initOption) {

	//整个过程中，客户端都是这些不会变化
	rf.peers = op.Peers
	rf.persister = op.Persister

	//初始化一些变量以后会用到，默认为0的都不列出来了
	rf.me = op.Me
	rf.votedFor = -1
	//raft节点启动的时候都是从节点
	rf.status = RaftStatus_Follower
	rf.nextIndex = make(map[int]int)
	rf.matchIndex = make(map[int]int)
	rf.heartbeatDuration = op.HeartbeatDuration
	rf.electionTimeoutDuration = op.ElectionTimeoutDuration
}

func (rf *Raft) setLastHeartbeatTime(t time.Time) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartbeatTime = t
}

func (rf *Raft) getLastHeartbeatTime() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastHeartbeatTime
}

func (rf *Raft) getTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) setTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) setElectionTimeoutDuration(dur time.Duration) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionTimeoutDuration = dur
}

func (rf *Raft) getElectionTimeoutDuration() time.Duration {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.electionTimeoutDuration
}

func (rf *Raft) becomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.status = RaftStatus_Leader
}

func (rf *Raft) becomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.status = RaftStatus_Leader
}

func (rf *Raft) becomeFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.status = RaftStatus_Leader
}

func (rf *Raft) getPeers() []*labrpc.ClientEnd {
	return rf.peers
}

func (rf *Raft) getMe() int {
	return rf.me
}

func (rf *Raft) voteFor(candidate int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = candidate
}

func (rf *Raft)

func (rf *Raft) followerLoop() {
	electionTimeoutDuration := rf.getElectionTimeoutDuration()
	for range time.Tick(electionTimeoutDuration) {
		lastHeartbeatTime := rf.getLastHeartbeatTime()
		if time.Since(lastHeartbeatTime) > electionTimeoutDuration {
			rf.becomeCandidate()
			break
		}
	}
}

func (rf *Raft) candidateLoop() {
	timer := time.NewTimer(0)
	me := rf.getMe()
	rf.voteFor(me)
	for {
		select {
		case <-timer.C:
			for member := range rf.getPeers() {
				if member == me {
					continue
				}
				args := &RequestVoteArgs{

				}
				rf.RequestVote()
			}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	op := &initOption{
		Peers:                   peers,
		Me:                      me,
		Persister:               persister,
		HeartbeatDuration:       time.Millisecond * 100,
		ElectionTimeoutDuration: time.Millisecond * 1000,
	}
	rf.init(op)

	go func() {
		heartDur := time.Millisecond * 100
		for {
			if rf.status == RaftStatus_Leader {
				time.Sleep(heartDur)
				for server := range peers {
					if server == me {
						continue
					}
					go func(follower int) {
						args := &AppendEntriesArgs{}
						args.Term = rf.currentTerm
						args.LeaderID = me
						args.Entries = []RaftLog{}
						args.LeaderCommit = rf.commitIndex

						reply := &AppendEntriesReply{}
						log.Printf("leader [%d] -> follower [%d], heartbeat request: term %d\n", me, follower, rf.currentTerm)
						if !rf.sendAppendEntries(follower, args, reply) {
							log.Printf("follower [%d] -> leader [%d], heart beat response timeout\n", follower, me)
						}
						log.Printf("follower [%d] -> leader [%d], heart beat response: term %d\n", follower, me, reply.Term)
					}(server)
				}
			} else if rf.status == RaftStatus_Follower {
				time.Sleep(heartDur * time.Duration(rand.Intn(5)))
				if time.Since(rf.lastHeartbeatTime) > heartDur*4 {
					rf.status = RaftStatus_Candidate
				}
			} else if rf.status == RaftStatus_Candidate {
				voted := 1
				for server := range peers {
					if server == me {
						continue
					}
					args := &RequestVoteArgs{}
					args.Term = rf.currentTerm
					args.CandidateID = me
					if len(rf.log) > 0 {
						thisLastLog := rf.log[len(rf.log)-1]
						args.LastLogIndex = len(rf.log) - 1
						args.LastLogTerm = thisLastLog.Term
					}

					reply := &RequestVoteReply{}
					log.Printf("server [%d] -> server [%d], request vote request: term %d\n", me, server, rf.currentTerm)
					if !rf.sendRequestVote(server, args, reply) {
						log.Printf("server [%d] -> server [%d], request vote response timeout\n", me, server)
					}
					if reply.VoteGranted {
						voted++
					}
					log.Printf("server [%d] -> server [%d], request vote response: term %d, success: %v\n", server, me, reply.Term, reply.VoteGranted)
				}
				if voted > len(peers)/2 {
					rf.status = RaftStatus_Leader
				} else {
					rf.currentTerm++
					time.Sleep(heartDur * time.Duration(rand.Intn(5)))
				}
			}
		}

	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
