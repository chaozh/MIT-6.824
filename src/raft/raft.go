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
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

const (
	HeartbeatInterval = time.Millisecond * 100
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
	RaftStatus_Follower  RaftStatus = iota + 1
	RaftStatus_Candidate RaftStatus = iota + 1
	RaftStatus_Leader    RaftStatus = iota + 1
)

type LogEntry struct {
	Term int
	Cmd  string
}

type PersistentState struct {
	currentTerm int
	votedFor    int
	log         []LogEntry
}

type VolatileState struct {
	commitIndex int
	lastApplied int
}

type LeaderState struct {
	nextIndex  map[int]int
	matchIndex map[int]int
}

type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status RaftStatus

	PersistentState

	VolatileState

	LeaderState

	electionDeadline time.Time
	electionTimeout  time.Duration

	cancel context.CancelFunc
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.Term(), rf.status == RaftStatus_Leader
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
/*
	1. 如果term < currentTerm返回 false （5.2 节）
    2. 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.VoteGranted = false
	if args.Term < rf.Term() {
		reply.Term = rf.Term()
		return
	} else if args.Term > rf.Term() {
		rf.BecomeFollower(args.Term, args.CandidateID)
		rf.ResetElectionDeadline()
		reply.VoteGranted = true
		return
	}

	log.Printf("%d request vote, term: %d\n", args.CandidateID, args.Term)
	log.Printf("%d has voted for %d, term: %d\n", rf.me, rf.votedFor, rf.currentTerm)

	if rf.VotedFor() != -1 && rf.VotedFor() != args.CandidateID {
		return
	}

	//index较大的，日志更新；同样index下，最后一条term较大的，日志更新；同样index和同样最后一条term情况下，日志更长的更新
	lastLogIndex := rf.LastLogIndex()
	if args.LastLogIndex < lastLogIndex {
		return
	} else if args.LastLogIndex == lastLogIndex {
		lastLogTerm := rf.LastLogTerm()
		if args.LastLogTerm < lastLogTerm {
			return
		}
	}

	rf.BecomeFollower(args.Term, args.CandidateID)
	rf.ResetElectionDeadline()
	reply.VoteGranted = true
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
func (rf *Raft) sendRequestVote(ctx context.Context, server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ch := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		ch <- ok
	}()
	select {
	case ok := <-ch:
		return ok
	case <-ctx.Done():
		return false
	}
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
	Entries      []LogEntry
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
	term := rf.Term()
	reply.Term = term
	if args.Term < term {
		reply.Success = false
		return
	} else if args.Term > term {
		// remote term is bigger than local
		rf.BecomeFollower(args.Term, args.LeaderID)
		rf.ResetElectionDeadline()
		reply.Success = true
		return
	}

	// heartbeat request with empty entry
	if len(args.Entries) == 0 {
		rf.ResetElectionDeadline()
		if rf.IsCandidate() {
			rf.BecomeFollower(args.Term, args.LeaderID)
		}
	}
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(ctx context.Context, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ch := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		ch <- ok
	}()
	select {
	case ok := <-ch:
		return ok
	case <-ctx.Done():
		return false
	}
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
	// Your code here (2B).
	nextIndex := rf.LastLogIndex() + 1
	term := rf.Term()
	isLeader := rf.IsLeader()

	return nextIndex, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.cancel()
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

func (rf *Raft) ElectionTimeout() time.Duration {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.electionTimeout
}

func (rf *Raft) ResetElectionTimedout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rand.Seed(time.Now().UnixNano())
	r := rand.Intn(200)
	rf.electionTimeout = 2*HeartbeatInterval + time.Duration(r)*time.Millisecond
	DPrintf("election timeout %v, %v", rf.electionTimeout, rf.me)
}

func (rf *Raft) ResetElectionDeadline() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.electionDeadline = time.Now().Add(rf.electionTimeout)
}

func (rf *Raft) ElectionDeadline() time.Time {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.electionDeadline
}

func (rf *Raft) Wait4ElectionTimeout(ctx context.Context) error {
	timer := time.NewTimer(rf.ElectionTimeout())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if rf.ElectionDeadline().Before(time.Now()) {
				// election timeout fire
				return nil
			}
		}
		timer.Reset(rf.ElectionTimeout())
	}
}

func (rf *Raft) Term() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.PersistentState.currentTerm
}

func (rf *Raft) SetTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.PersistentState.currentTerm = term
}

func (rf *Raft) IncrTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.PersistentState.currentTerm++
}

func (rf *Raft) LastLogIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) == 0 {
		return 0
	}
	return len(rf.log) - 1
}

func (rf *Raft) LastLogTerm() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if len(rf.log) == 0 {
		return 0
	}
	lastLog := rf.log[len(rf.log)-1]
	return lastLog.Term
}

func (rf *Raft) BecomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.status = RaftStatus_Leader
}

func (rf *Raft) BecomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.status = RaftStatus_Candidate
}

func (rf *Raft) BecomeFollower(term, leaderID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.PersistentState.currentTerm = term
	rf.votedFor = leaderID
	rf.status = RaftStatus_Follower
}

func (rf *Raft) Peers() []*labrpc.ClientEnd {
	return rf.peers
}

func (rf *Raft) Me() int {
	return rf.me
}

func (rf *Raft) VotedFor() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.votedFor
}
func (rf *Raft) VoteFor(candidateID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = candidateID
}

func (rf *Raft) IsLeader() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.status == RaftStatus_Leader
}

func (rf *Raft) IsFollower() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.status == RaftStatus_Follower
}
func (rf *Raft) IsCandidate() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.status == RaftStatus_Candidate
}

func (rf *Raft) Log(id int) (LogEntry, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if id >= len(rf.log) {
		return LogEntry{}, false
	}
	return rf.log[id], true
}

func (rf *Raft) FollowerLoop(ctx context.Context) {
	if !rf.IsFollower() {
		return
	}

	for rf.IsFollower() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := rf.Wait4ElectionTimeout(ctx); err != nil {
			return
		}
		rf.BecomeCandidate()
		return
	}
}

func (rf *Raft) CandidateLoop(ctx context.Context) {
	if !rf.IsCandidate() {
		return
	}

	me := rf.Me()
	count := len(rf.Peers())
	timer := time.NewTimer(rf.ElectionTimeout())
	defer timer.Stop()
	for rf.IsCandidate() {
		log.Println("candidate...", rf.me)
		select {
		case <-ctx.Done():
			return
		default:
		}
		rf.IncrTerm()
		rf.ResetElectionDeadline()
		rf.VoteFor(me)

		resCh := make(chan *RequestVoteReply, count-1)
		args := &RequestVoteArgs{
			Term:         rf.Term(),
			CandidateID:  me,
			LastLogIndex: rf.LastLogIndex(),
			LastLogTerm:  rf.LastLogTerm(),
		}

		for peer := 0; peer < count; peer++ {
			if peer == me {
				// skip requesting vote to myself
				continue
			}
			go func(peer int) {
				ctx, cancel := context.WithTimeout(ctx, HeartbeatInterval)
				defer cancel()
				reply := &RequestVoteReply{}
				if !rf.sendRequestVote(ctx, peer, args, reply) {
					DPrintf("SendRequestVote failed, %d -> %d", me, peer)
				}
				resCh <- reply
				log.Printf("%d -> %d, %v\n", me, peer, reply.VoteGranted)
			}(peer)
		}

		votes := 1
		for peer := 0; peer < count-1; peer++ {
			select {
			case <-ctx.Done():
				return
			case res := <-resCh:
				if res.Term > rf.Term() {
					rf.BecomeFollower(res.Term, -1)
					rf.ResetElectionDeadline()
					return
				}

				if res.VoteGranted {
					votes++
				}
			}
		}
		if votes > count/2 {
			rf.BecomeLeader()
			return
		}
		<-timer.C
		rf.ResetElectionTimedout()
		timer.Reset(rf.ElectionTimeout())
	}
}

func (rf *Raft) LeaderLoop(ctx context.Context) {
	if !rf.IsLeader() {
		return
	}

	count := len(rf.Peers())
	me := rf.Me()
	ticker := time.NewTicker(rf.ElectionTimeout())
	defer ticker.Stop()
	for rf.IsLeader() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		args := &AppendEntriesArgs{
			Term: rf.Term(),
		}
		resCh := make(chan *AppendEntriesReply, count-1)
		for peer := 0; peer < count; peer++ {
			if peer == me {
				//skip myself
				continue
			}
			go func(peer int) {
				reply := &AppendEntriesReply{}
				ctx, cancel := context.WithTimeout(ctx, HeartbeatInterval)
				defer cancel()
				if !rf.sendAppendEntries(ctx, peer, args, reply) {
					DPrintf("SendAppendEntries failed, %d -> %d", me, peer)
				}
				resCh <- reply
			}(peer)
		}

		for peer := 0; peer < count-1; peer++ {
			reply := <-resCh
			if reply.Term > rf.Term() {
				rf.BecomeFollower(reply.Term, -1)
				rf.ResetElectionDeadline()
				return
			}
		}
		<-ticker.C
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		me:        me,
		persister: persister,
	}

	ctx, cancel := context.WithCancel(context.Background())
	rf.cancel = cancel

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// startup as follower
	rf.BecomeFollower(0, -1)
	rf.ResetElectionDeadline()
	rf.ResetElectionTimedout()

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			rf.LeaderLoop(ctx)
			rf.FollowerLoop(ctx)
			rf.CandidateLoop(ctx)
		}
	}()

	return rf
}
