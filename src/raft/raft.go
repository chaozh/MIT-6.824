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
	"encoding/json"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	HeartbeatInterval          = time.Millisecond * 100
	AppendEntriesRetryInterval = time.Millisecond * 200
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
	logs        []LogEntry

	//VolatileState
	commitIndex  int
	lastApplied  int
	newApplied   chan ApplyMsg
	newAppended  chan struct{}
	newCommitted chan struct{}

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	reply.VoteGranted = false
	term := rf.currentTerm
	if args.Term < term {
		reply.Term = term
		return
	} else if args.Term > term {
		lastLog, id := rf.lastLog()
		if args.LastLogTerm < lastLog.Term {
			return
		}
		if args.LastLogIndex < id {
			return
		}
		rf.resetElectionDeadline()
		rf.becomeFollower(args.Term, args.CandidateID)
		reply.VoteGranted = true
		return
	}

	DPrintf("%d request vote, term: %d\n", args.CandidateID, args.Term)
	DPrintf("%d has voted for %d, term: %d\n", rf.Me(), rf.votedFor, rf.currentTerm)

	if rf.votedFor != -1 && rf.votedFor != args.CandidateID {
		return
	}

	//index较大的，日志更新；同样index下，最后一条term较大的，日志更新；同样index和同样最后一条term情况下，日志更长的更新
	_, lastLogIndex := rf.lastLog()
	if args.LastLogIndex < lastLogIndex {
		return
	} else if args.LastLogIndex == lastLogIndex {
		lastLog, _ := rf.lastLog()
		if args.LastLogTerm < lastLog.Term {
			return
		}
	}

	rf.resetElectionDeadline()
	rf.becomeFollower(args.Term, args.CandidateID)
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
		rf.becomeFollower(args.Term, args.LeaderID)
		rf.resetElectionDeadline()
		reply.Success = true
	}

	if len(args.Entries) == 0 {
		// heartbeat request with empty entry
		rf.resetElectionDeadline()
		if rf.isCandidate() {
			rf.becomeFollower(args.Term, args.LeaderID)
			return
		}
	} else {
		// handle entries
		prevEntry := rf.log(args.PrevLogIndex)
		if prevEntry.Term != args.PrevLogTerm {
			return
		}
		rf.appendEntry(args.Entries...)
	}

	rf.maybeFollowerCommit(args.LeaderCommit)
	rf.maybeApplyEntry()
	DPrintf("%v logs %v", rf.me, rf.logs)
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
	term := rf.Term()
	isLeader := rf.IsLeader()
	if !isLeader {
		return -1, term, isLeader
	}

	entry := LogEntry{
		Term: term,
		Cmd:  command,
	}

	DPrintf("%d append entry, term: %d , cmd: %v", rf.Me(), rf.Term(), command)
	lastLogID := rf.AppendEntry(entry)

	nc := rf.NewCommitted()
	for {
		select {
		case <-nc:
		}
		nc = rf.NewCommitted()
		if lastLogID <= rf.CommitIndex() {
			break
		}
	}
	DPrintf("command %v, commited in %v", command, lastLogID)
	_, nextIndex := rf.LastLog()
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

func (rf *Raft) NewCommit() {
	close(rf.newCommitted)
	rf.newCommitted = make(chan struct{})
}

func (rf *Raft) NewCommitted() <-chan struct{} {
	return rf.newCommitted
}

func (rf *Raft) NewAppend() {
	close(rf.newAppended)
	rf.newAppended = make(chan struct{})
}

func (rf *Raft) NewAppended() <-chan struct{} {
	return rf.newAppended
}

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
}

func (rf *Raft) ResetElectionDeadline() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionDeadline()
}

func (rf *Raft) resetElectionDeadline() {
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

func (rf *Raft) maybeFollowerCommit(leaderCommit int) {
	if rf.commitIndex < leaderCommit {
		// leaderCommit is bigger than commitIndex,
		// the local commitIndex equal
		if len(rf.logs) != 0 && leaderCommit > len(rf.logs)-1 {
			rf.commitIndex = len(rf.logs) - 1
		} else {
			rf.commitIndex = leaderCommit
		}

		DPrintf("follower %d new commit index: %v", rf.me, rf.commitIndex)
	}
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
	entry := rf.logs[rf.lastApplied]
	applyMsg := ApplyMsg{
		Index:   rf.lastApplied,
		Command: entry.Cmd,
	}

	rf.newApplied <- applyMsg
}

func (rf *Raft) Me() int {
	return rf.me
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

func (rf *Raft) LastLog() (LogEntry, int) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.lastLog()
}

func (rf *Raft) lastLog() (LogEntry, int) {
	lastLog := rf.logs[len(rf.logs)-1]
	return lastLog, len(rf.logs) - 1
}

func (rf *Raft) BecomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.becomeLeader()
}

func (rf *Raft) becomeLeader() {
	rf.status = RaftStatus_Leader
}

func (rf *Raft) BecomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.becomeCandidate()
}

func (rf *Raft) becomeCandidate() {
	rf.status = RaftStatus_Candidate
}

func (rf *Raft) BecomeFollower(term, leaderID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.becomeFollower(term, leaderID)
}

func (rf *Raft) becomeFollower(term, leaderID int) {
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
	return rf.isLeader()
}

func (rf *Raft) isLeader() bool {
	return rf.status == RaftStatus_Leader
}

func (rf *Raft) IsFollower() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.isFollower()
}

func (rf *Raft) isFollower() bool {
	return rf.status == RaftStatus_Follower
}

func (rf *Raft) IsCandidate() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.isCandidate()
}

func (rf *Raft) isCandidate() bool {
	return rf.status == RaftStatus_Candidate
}

func (rf *Raft) AppendEntry(entry LogEntry) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.appendEntry(entry)
}

func (rf *Raft) appendEntry(entries ...LogEntry) int {
	rf.logs = append(rf.logs, entries...)
	rf.NewAppend()
	return len(rf.logs) - 1
}

func (rf *Raft) LogAfter(idx int) []LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	if idx >= len(rf.logs) {
		return nil
	}
	return rf.logs[idx:]
}

func (rf *Raft) Log(idx int) LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.log(idx)
}

func (rf *Raft) log(idx int) LogEntry {
	if idx >= len(rf.logs) || idx == -1 {
		return LogEntry{
			Term: 0,
		}
	}
	return rf.logs[idx]
}

func (rf *Raft) InitNextIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	me := rf.Me()
	count := len(rf.Peers())
	rf.nextIndex = make(map[int]int)
	for i := 0; i < count; i++ {
		if i == me {
			continue
		}
		_, lastLogIndex := rf.lastLog()
		rf.nextIndex[i] = lastLogIndex + 1
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

	DPrintf("follower %v...", rf.Me())

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
	for rf.IsCandidate() {
		DPrintf("candidate... %d", rf.Me())
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
		rf.resetElectionDeadline()
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

func (rf *Raft) leaderSendEntriesLoop(ctx context.Context) {
	if !rf.IsLeader() {
		return
	}

	count := len(rf.Peers())
	me := rf.Me()
	replyCh := make(chan *AppendEntriesReply, count-1)
	newMatched := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for peer := 0; peer < count; peer++ {

		if peer == me {
			//skip myself
			continue
		}

		go func(peer int) {
			na := rf.NewAppended()
			for rf.IsLeader() {

				select {
				case <-na:
				}

				// avoid missing trigger
				na = rf.NewAppended()

			Retry:
				select {
				case <-ctx.Done():
					return
				default:
				}

				index := rf.NextIndex(peer)
				_, lastLogIndex := rf.LastLog()

				if index == lastLogIndex+1 {
					continue
				}

				entries := rf.LogAfter(index)
				prevEntry := rf.Log(index - 1)

				ci := rf.CommitIndex()
				term := rf.Term()
				args := &AppendEntriesArgs{
					Term:         term,
					LeaderID:     me,
					Entries:      entries,
					LeaderCommit: ci,
					PrevLogIndex: index - 1,
					PrevLogTerm:  prevEntry.Term,
				}
				data, _ := json.Marshal(args)
				DPrintf(" append req %s", data)
				reply := &AppendEntriesReply{}
				if !rf.sendAppendEntries(ctx, peer, args, reply) {
					DPrintf("SendAppendEntries failed, %d -> %d", me, peer)
					time.Sleep(AppendEntriesRetryInterval)
					goto Retry
				}

				if !reply.Success && reply.Term <= rf.Term() {
					rf.SetNextIndex(peer, index-1)
					goto Retry
				} else if reply.Success {
					rf.SetNextIndex(peer, index+len(entries))
					rf.SetMatchIndex(peer, index+len(entries)-1)
					close(newMatched)
					newMatched = make(chan struct{})
				}

				select {
				case <-ctx.Done():
					return
				case replyCh <- reply:
				}
			}
		}(peer)
	}

	go func() {
		newMatchedOnce := newMatched
		for {
			select {
			case <-newMatchedOnce:
			}
			// avoid missing new matched
			newMatchedOnce = newMatched

			_, lastLogID := rf.LastLog()
			for i := lastLogID; i > rf.CommitIndex(); i-- {

				select {
				case <-ctx.Done():
					return
				default:
				}

				entry := rf.Log(i)
				if entry.Term != rf.Term() {
					break
				}

				matched := 1
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
					rf.NewCommit()
					rf.MaybeApplyEntry()
					break
				}
			}
		}
	}()

	for reply := range replyCh {
		if reply.Term > rf.Term() {
			cancel()
			rf.BecomeFollower(reply.Term, -1)
			rf.ResetElectionDeadline()
		}
	}
}

func (rf *Raft) leaderHeartbeatLoop(ctx context.Context) {
	if !rf.IsLeader() {
		return
	}

	rf.InitNextIndex()
	me := rf.Me()
	count := len(rf.Peers())

	DPrintf("leader... %d", rf.Me())
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
					DPrintf("Heartbeat failed, %d -> %d", me, peer)
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

func (rf *Raft) LeaderLoop(ctx context.Context) {
	if !rf.IsLeader() {
		return
	}
	go rf.leaderHeartbeatLoop(ctx)
	go rf.leaderSendEntriesLoop(ctx)
	select {
	case <-ctx.Done():
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:        peers,
		me:           me,
		persister:    persister,
		commitIndex:  0,
		lastApplied:  0,
		newApplied:   applyCh,
		newAppended:  make(chan struct{}),
		newCommitted: make(chan struct{}),
		// init with a empty log
		logs: []LogEntry{{0, nil}},
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
