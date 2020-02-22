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
	Cmd  interface{}
}

type Raft struct {
	// PersistentState
	currentTerm int
	votedFor    int
	log         []LogEntry

	//VolatileState
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg

	//LeaderState
	nextIndex  map[int]int
	matchIndex map[int]int

	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status RaftStatus

	electionDeadline time.Time
	electionTimeout  time.Duration

	cancel context.CancelFunc
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.Term(), rf.IsLeader()
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
	rf.requestVote(args, reply)
	DPrintf("%d, %d -> %d, %d, %v", args.CandidateID, args.Term, rf.me, rf.currentTerm, reply.VoteGranted)
}
func (rf *Raft) requestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	reply.VoteGranted = false
	term := rf.currentTerm
	if args.Term < term {
		reply.Term = term
		return
	} else if args.Term > term {
		rf.resetElectionDeadlineNoLock()
		rf.becomeFollowerNolock(args.Term, args.CandidateID)
		reply.VoteGranted = true
		return
	}

	DPrintf("%d request vote, term: %d\n", args.CandidateID, args.Term)
	DPrintf("%d has voted for %d, term: %d\n", rf.me, rf.votedFor, rf.currentTerm)

	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		return
	}

	//index较大的，日志更新；同样index下，最后一条term较大的，日志更新；同样index和同样最后一条term情况下，日志更长的更新
	lastLogIndex := rf.lastLogIndexNolock()
	if args.LastLogIndex < lastLogIndex {
		return
	} else if args.LastLogIndex == lastLogIndex {
		lastLog, _ := rf.lastLog()
		if args.LastLogTerm < lastLog.Term {
			return
		}
	}

	rf.resetElectionDeadlineNoLock()
	rf.becomeFollowerNolock(args.Term, args.CandidateID)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	reply.Term = term
	if args.Term < term {
		reply.Success = false
		return
	} else if args.Term > term {
		// remote term is bigger than local
		rf.becomeFollowerNolock(args.Term, args.LeaderID)
		rf.resetElectionDeadlineNoLock()
		reply.Success = true
	}

	// heartbeat request with empty entry
	if len(args.Entries) == 0 {
		rf.resetElectionDeadlineNoLock()
		if rf.isCandidateNolock() {
			rf.becomeFollowerNolock(args.Term, args.LeaderID)
			return
		}
	} else {
		prevEntry := rf.logNolock(args.PrevLogIndex)
		if prevEntry.Term != args.PrevLogTerm {
			return
		}
		rf.appendEntry(args.Entries...)
	}

	if rf.commitIndex < args.LeaderCommit {
		if len(rf.log) != 0 && args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}

		DPrintf("follower new commit index: %v", rf.commitIndex)
		rf.maybeApplyEntry()
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
	rf.mu.Lock()
	term := rf.currentTerm
	isLeader := rf.isLeaderNolock()
	if !isLeader {
		rf.mu.Unlock()
		return -1, term, isLeader
	}

	entry := LogEntry{
		Term: term,
		Cmd:  command,
	}

	rf.appendEntry(entry)
	rf.mu.Unlock()

	count := len(rf.Peers())
	me := rf.me
	ci := rf.CommitIndex()
	resCh := make(chan *AppendEntriesReply, count-1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for peer := 0; peer < count; peer++ {
		go func(peer int) {
			if peer == me {
				//skip myself
				return
			}

		Retry:
			select {
			case <-ctx.Done():
				return
			default:
			}
			index := rf.NextIndex(peer)
			entries := rf.LogAfter(index)
			prevEntry := rf.Log(index - 1)

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     me,
				Entries:      entries,
				LeaderCommit: ci,
				PrevLogIndex: index - 1,
				PrevLogTerm:  prevEntry.Term,
			}
			reply := &AppendEntriesReply{}
			if !rf.sendAppendEntries(ctx, peer, args, reply) {
				DPrintf("SendAppendEntries failed, %d -> %d", me, peer)
			}
			DPrintf("append %d -> %d, term: %d , cmd: %v, ok: %v", me, peer, term, command, reply.Success)
			if !reply.Success && reply.Term <= rf.Term() {
				rf.SetNextIndex(peer, index-1)
				goto Retry
			} else if reply.Success {
				rf.SetNextIndex(peer, index+len(entries))
				rf.SetMatchIndex(peer, index+len(entries)-1)
			}
			resCh <- reply
		}(peer)
	}

	for peer := 0; peer < count; peer++ {
		if peer == me {
			continue
		}
		reply := <-resCh
		if reply.Term > rf.Term() {
			cancel()
			rf.BecomeFollower(reply.Term, -1)
			rf.ResetElectionDeadline()
			nextIndex := rf.lastLogIndexNolock() + 1
			return nextIndex, term, false
		}
	}

	_, lastLogID := rf.LastLog()
	DPrintf("lastlogID %d, commitid %d", lastLogID, rf.CommitIndex())
	for i := lastLogID; i > rf.CommitIndex(); i-- {
		entry := rf.Log(i)
		if entry.Term != rf.Term() {
			break
		}

		matched := 0
		for ii := 0; ii < count; ii++ {
			if ii == me {
				continue
			}
			if rf.MatchIndex(ii) >= i {
				matched++
			}
		}
		if matched > count/2 {
			rf.SetCommitIndex(i)
			rf.MaybeApplyEntry()
			break
		}
	}

	nextIndex := rf.LastLogIndex()
	DPrintf("nextIndex: %d, term: %d, isLeader: %v", nextIndex, term, isLeader)
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
	rf.resetElectionDeadlineNoLock()
}

func (rf *Raft) resetElectionDeadlineNoLock() {
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

func (rf *Raft) CommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) SetCommitIndex(ci int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = ci
}

func (rf *Raft) MaybeApplyEntry() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.maybeApplyEntry()
}

func (rf *Raft) maybeApplyEntry() {
	if rf.commitIndex <= rf.lastApplied {
		return
	}
	rf.lastApplied++
	entry := rf.log[rf.lastApplied]
	applyMsg := ApplyMsg{
		Index:   rf.lastApplied,
		Command: entry.Cmd,
	}

	rf.applyCh <- applyMsg
}

func (rf *Raft) Term() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm
}

func (rf *Raft) SetTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = term
}

func (rf *Raft) LastLogIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastLogIndexNolock()
}

func (rf *Raft) lastLogIndexNolock() int {
	return len(rf.log) - 1
}

func (rf *Raft) LastLog() (LogEntry, int) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.lastLog()
}

func (rf *Raft) lastLog() (LogEntry, int) {
	lastLog := rf.log[len(rf.log)-1]
	return lastLog, len(rf.log) - 1
}

func (rf *Raft) BecomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.becomeLeaderNolock()
}

func (rf *Raft) becomeLeaderNolock() {
	rf.status = RaftStatus_Leader
}

func (rf *Raft) BecomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.becomeCandidateNolock()
}

func (rf *Raft) becomeCandidateNolock() {
	rf.status = RaftStatus_Candidate
}

func (rf *Raft) BecomeFollower(term, leaderID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.becomeFollowerNolock(term, leaderID)
}

func (rf *Raft) becomeFollowerNolock(term, leaderID int) {
	rf.currentTerm = term
	rf.votedFor = leaderID
	rf.status = RaftStatus_Follower
}

func (rf *Raft) Peers() []*labrpc.ClientEnd {
	return rf.peers
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
	return rf.isLeaderNolock()
}

func (rf *Raft) isLeaderNolock() bool {
	return rf.status == RaftStatus_Leader
}

func (rf *Raft) IsFollower() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.isFollowerNolock()
}

func (rf *Raft) isFollowerNolock() bool {
	return rf.status == RaftStatus_Follower
}

func (rf *Raft) IsCandidate() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.isCandidateNolock()
}

func (rf *Raft) isCandidateNolock() bool {
	return rf.status == RaftStatus_Candidate
}

func (rf *Raft) AppendEntry(entry LogEntry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.appendEntry(entry)
}

func (rf *Raft) appendEntry(entries ...LogEntry) {
	rf.log = append(rf.log, entries...)
}

func (rf *Raft) LogAfter(idx int) []LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if idx >= len(rf.log) {
		return nil
	}
	return rf.log[idx:]
}

func (rf *Raft) Log(idx int) LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.logNolock(idx)
}

func (rf *Raft) logNolock(idx int) LogEntry {
	if idx >= len(rf.log) || idx == -1 {
		return LogEntry{
			Term: 0,
		}
	}
	return rf.log[idx]
}

func (rf *Raft) InitNextIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	me := rf.me
	count := len(rf.Peers())
	rf.nextIndex = make(map[int]int)
	for i := 0; i < count; i++ {
		if i == me {
			continue
		}
		rf.nextIndex[i] = rf.lastLogIndexNolock() + 1
	}
}

func (rf *Raft) NextIndex(sever int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[sever]
}

func (rf *Raft) SetNextIndex(sever, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[sever] = index
}

func (rf *Raft) SetMatchIndex(server, idx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.matchIndex == nil {
		rf.matchIndex = make(map[int]int)
	}
	rf.matchIndex[server] = idx
}

func (rf *Raft) MatchIndex(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIndex[server]
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

	me := rf.me
	count := len(rf.Peers())
	for rf.IsCandidate() {
		DPrintf("candidate... %d", rf.me)
		select {
		case <-ctx.Done():
			return
		default:
		}

		rf.mu.Lock()
		entry, id := rf.lastLog()
		lastLogIndex := id
		lastLogTerm := entry.Term
		rf.currentTerm++
		term := rf.currentTerm
		rf.resetElectionDeadlineNoLock()
		rf.votedFor = me
		rf.mu.Unlock()

		resCh := make(chan *RequestVoteReply, count-1)
		args := &RequestVoteArgs{
			Term:         term,
			CandidateID:  me,
			LastLogIndex: lastLogIndex,
			LastLogTerm:  lastLogTerm,
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
			}(peer)
		}

		votes := 1
		for peer := 0; peer < count-1; peer++ {
			select {
			case <-ctx.Done():
				return
			case res := <-resCh:
				if term != rf.Term() {
					return
				}
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
			if term == rf.Term() {
				rf.BecomeLeader()
			}
			return
		}
		rf.ResetElectionTimedout()
		time.Sleep(rf.ElectionTimeout())
	}
}

func (rf *Raft) LeaderLoop(ctx context.Context) {
	if !rf.IsLeader() {
		return
	}

	rf.InitNextIndex()
	me := rf.me
	count := len(rf.Peers())

	DPrintf("leader... %d", rf.me)
	ticker := time.NewTicker(rf.ElectionTimeout())
	defer ticker.Stop()
	for rf.IsLeader() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		term := rf.Term()
		resCh := make(chan *AppendEntriesReply, count-1)
		for peer := 0; peer < count; peer++ {

			index := rf.NextIndex(peer)
			prevEntry := rf.Log(index - 1)
			ci := rf.CommitIndex()

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderID:     me,
				LeaderCommit: ci,
				PrevLogIndex: index - 1,
				PrevLogTerm:  prevEntry.Term,
			}

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

		for peer := 0; peer < count; peer++ {
			if peer == me {
				continue
			}
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
		peers:       peers,
		me:          me,
		persister:   persister,
		commitIndex: 0,
		lastApplied: 0,
		applyCh:     applyCh,
		// init with a empty log
		log: []LogEntry{{0, nil}},
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
