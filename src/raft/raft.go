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
	//	"bytes"

	"context"
	"math/rand"
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

//
// A Go object implementing a single Raft peer.
//
type Role int32

const (
	FollowerRole = iota
	CandidateRole
	LeaderRole
)

const (
	electionTimeout    = 500
	heartbeatInterval  = 150
	heartbeatTimeout   = 150
	requestvoteTimeout = 150
)

type LogEntry struct {
	Term    int
	Command struct {
		Action int
		Key    int
		Value  int
	}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	role Role

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []LogEntry
	leaderid    int

	commitIndex int
	lastApplied int
	*Leaderstate
	updatetime bool
}

type Leaderstate struct {
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
	DPrintf("%d: RequestVote(Term:%d, CandidateID:%d, LIndex:%d)\n", rf.me, args.Term, args.CandidateID, args.LastLogIndex)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		DPrintf("%d: ReplyVote(Term:%d, VoteGranted:%t, votedFor:%d)\n", rf.me, reply.Term, reply.VoteGranted, rf.votedFor)
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FollowerRole
		rf.votedFor = -1
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		currentLogIndex := len(rf.log) - 1
		currentLogTerm := rf.log[currentLogIndex].Term
		DPrintf("%d: currentLogIndex:%d, currentLogTerm:%d, args.LastLogIndex:%d, args.LastLogTerm:%d\n", rf.me, currentLogIndex, currentLogTerm, args.LastLogIndex, args.LastLogTerm)
		if args.LastLogIndex < currentLogIndex || (args.LastLogIndex == currentLogIndex && args.LastLogTerm < currentLogTerm) {
			DPrintf("%d: ReplyVote(Term:%d, VoteGranted:%t, votedFor:%d)\n", rf.me, reply.Term, reply.VoteGranted, rf.votedFor)
			return
		}
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	}
	DPrintf("%d: ReplyVote(Term:%d, VoteGranted:%t, votedFor:%d)\n", rf.me, reply.Term, reply.VoteGranted, rf.votedFor)
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
	PervLogIndex int
	PervLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesRes struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesRes) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = FollowerRole
		rf.votedFor = -1
	}
	//DPrintf("%d: AppendEntries(%d, %d, %d, %d, %d)\n", rf.me, args.Term, args.LeaderId, args.PervLogIndex, args.PervLogTerm, len(args.Entries))
	rf.updatetime = true
	if len(rf.log)-1 < args.PervLogIndex || rf.log[args.PervLogIndex].Term != args.Term {
		reply.Success = false
		return
	}
	if len(rf.log)-1 > args.PervLogIndex && rf.log[args.PervLogIndex+1].Term != args.Term {
		rf.log = rf.log[:args.PervLogIndex+1]
	}
	rf.log = append(rf.log, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
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
	timeout := rand.Int()%electionTimeout + electionTimeout
	for !rf.killed() {
		rf.mu.Lock()
		if rf.updatetime {
			rf.updatetime = false
			t = time.Now()
			rf.mu.Unlock()
		} else if time.Since(t) > time.Duration(timeout)*time.Millisecond {
			rf.mu.Unlock()
			go rf.startElection()
			return
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(rand.Int()%electionTimeout+electionTimeout) * time.Millisecond)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.role = CandidateRole
	rf.currentTerm++
	rf.votedFor = rf.me
	LastLogIndex := len(rf.log) - 1
	voteCount := int32(len(rf.peers)/2 + 1)
	DPrintf("%d: start a new election, term %d, voteCount %d\n", rf.me, rf.currentTerm, voteCount)
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(len(rf.peers))
	term := rf.currentTerm
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			atomic.AddInt32(&voteCount, -1)
			waitGroup.Done()
			continue
		}
		go func(i int, waitGroup *sync.WaitGroup) {
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateID:  rf.me,
				LastLogIndex: LastLogIndex,
				LastLogTerm:  rf.log[LastLogIndex].Term,
			}
			reply := RequestVoteReply{}
			ctx, cancle := context.WithTimeout(context.Background(), time.Duration(requestvoteTimeout)*time.Millisecond)
			go func(cancle context.CancelFunc, i int) {
				if ok := rf.sendRequestVote(i, &args, &reply); ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.VoteGranted {
						atomic.AddInt32(&voteCount, -1)
						if atomic.LoadInt32(&voteCount) == 0 {
							rf.role = LeaderRole
							DPrintf("%d: become leader, term %d\n", rf.me, rf.currentTerm)
							go rf.startHeartbeat()
						}
						cancle()
					}
					DPrintf("term:%d %d: send RequestVote to %d", rf.currentTerm, rf.me, i)
				} else {
					DPrintf("%d: send RequestVote to %d failed", rf.me, i)
				}
			}(cancle, i)
			<-ctx.Done()
			if ctx.Err() != nil {
				DPrintf("%d: send RequestVote to %d timeout", rf.me, i)
			}
			waitGroup.Done()
		}(i, &waitGroup)
	}
	waitGroup.Wait()
	DPrintf("%d: election finished, term %d,voteCount:%d\n", rf.me, term, voteCount)
	rf.mu.Lock()
	if rf.role == CandidateRole {
		go rf.ticker()
	}
	rf.mu.Unlock()
}

func (rf *Raft) startHeartbeat() {
	DPrintf("%d: start heartbeat", rf.me)
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role != LeaderRole {
			rf.mu.Unlock()
			break
		}
		LastLogIndex := len(rf.log) - 1
		if LastLogIndex < 0 {
			LastLogIndex = 0
		}
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PervLogIndex: LastLogIndex,
					PervLogTerm:  rf.log[LastLogIndex].Term,
					Entries:      []LogEntry{},
					LeaderCommit: rf.commitIndex,
				}
				rf.mu.Unlock()
				reply := AppendEntriesRes{}
				//DPrintf("%d: send heartbeat to %d", rf.me, i)
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(heartbeatTimeout)*time.Millisecond)
				go func(ctx context.Context, i int) {
					if ok := rf.sendAppendEntries(i, &args, &reply); ok {
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.Term > rf.currentTerm {
							DPrintf("%d: heartbeat to %d failed, reply term %d > current term %d", rf.me, i, reply.Term, rf.currentTerm)
							rf.currentTerm = reply.Term
							rf.role = FollowerRole
							DPrintf("%d: become follower, term %d", rf.me, rf.currentTerm)
							cancel()
							return
						}
						DPrintf("%d: receive heartbeat reply from %d,%v,%v", rf.me, i, args, reply)
					} else {
						DPrintf("%d: send heartbeat to %d failed", rf.me, i)
					}
				}(ctx, i)
				<-ctx.Done()
				// if ctx.Err() != nil {
				// 	DPrintf("%d: heartbeat to %d timeout", rf.me, i)
				// }
			}(i)
		}
		time.Sleep(time.Duration(heartbeatInterval) * time.Millisecond)
	}
	DPrintf("%d: stop heartbeat", rf.me)
	go rf.ticker()
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
	rf.log = make([]LogEntry, 1)
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.role = FollowerRole
	rf.updatetime = false
	rf.votedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
