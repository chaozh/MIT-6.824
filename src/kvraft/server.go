package kvraft

import (
	"bytes"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

const proportion = 9

func DPrintf(format string, a ...interface{}) (n int, err error) {
	str := strings.Join([]string{"[kvraft]:", format}, "")
	if Debug {
		log.Printf(str, a...)
	}
	return
}

func Max(a ...int64) int64 {
	ans := a[0]
	for _, v := range a {
		if v > ans {
			ans = v
		}
	}
	return ans
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType   string // "Put" or "Append" or "Get"
	Key      string
	Value    string
	ClientID int64
	Seq      int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate          int // snapshot if log grows this big
	RaftlastIncludedIndex int

	// Your definitions here.
	kvMap   map[string]string    //DB
	WaitMap map[int]chan WaitMsg //key:index in raft, value:channel
	seqMap  map[int64]int64
}

type WaitMsg struct {
	Err Err
	Op  Op
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	op := Op{
		OpType:   "Get",
		Key:      args.Key,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	}
	logindex, _, _ := kv.rf.Start(op)
	kv.mu.Unlock()

	kv.mu.Lock()
	DPrintf("[%d]: Get: %s", kv.me, args.Key)
	chForIndex, exist := kv.WaitMap[logindex]
	if !exist {
		kv.WaitMap[logindex] = make(chan WaitMsg, 1)
		chForIndex = kv.WaitMap[logindex]
	}
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.WaitMap, logindex)
		kv.mu.Unlock()
	}()

	DPrintf("[%d]: Get wait: %s", kv.me, args.Key)
	select {
	case waitmsg := <-chForIndex:
		if waitmsg.Op.Key != args.Key || waitmsg.Op.ClientID != args.ClientID || waitmsg.Op.Seq != args.Seq {
			DPrintf("[%d]: Get Wrong RPC: %s, %s", kv.me, args.Key, waitmsg.Err)
			reply.Err = ErrWrongLeader
			return
		}
		DPrintf("[%d]: Get RPC: %s->'%s'", kv.me, args.Key)
		if waitmsg.Err == OK {
			kv.mu.Lock()
			reply.Value = kv.kvMap[args.Key]
			kv.mu.Unlock()
			reply.Err = OK
		} else {
			reply.Err = waitmsg.Err
		}
		return
	case <-time.After(time.Duration(500) * time.Millisecond):
		DPrintf("[%d]: Get Timeout: %s", kv.me, args.Key)
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		OpType:   args.Op,
		Key:      args.Key,
		ClientID: args.ClientID,
		Seq:      args.Seq,
		Value:    args.Value,
	}
	index, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	DPrintf("[%d]: PutAppend: %s: %s->'%s'", kv.me, args.Op, args.Key, args.Value)
	chForIndex, exist := kv.WaitMap[index]
	if !exist {
		kv.WaitMap[index] = make(chan WaitMsg, 1)
		chForIndex = kv.WaitMap[index]
	}
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.WaitMap, index)
		kv.mu.Unlock()
	}()

	DPrintf("[%d]: PutAppend wait: %s: %s->'%s'", kv.me, args.Op, args.Key, args.Value)
	select {
	case waitmsg := <-chForIndex:
		if waitmsg.Op.Key != args.Key || waitmsg.Op.ClientID != args.ClientID || waitmsg.Op.Seq != args.Seq {
			DPrintf("[%d]: Put Append Wrong RPC: %s, %s", kv.me, args.Key, waitmsg.Err)
			reply.Err = ErrWrongLeader
			return
		}
		DPrintf("[%d]: Put Append %s RPC: %s->'%s', %s", kv.me, args.Op, args.Key, args.Value, waitmsg.Err)
		reply.Err = waitmsg.Err
		return
	case <-time.After(time.Duration(500) * time.Millisecond):
		DPrintf("[%d]: %s Timeout: %s", kv.me, args.Op, args.Key, args.Value)
		reply.Err = ErrWrongLeader
		return
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	DPrintf("[%d]: Kill.", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			DPrintf("[%d]: Applier apply: %v", kv.me, msg.Command)
			kv.ApplyCommandMsg(msg)
		} else if msg.SnapshotValid {
			DPrintf("[%d]: Applier apply: snapshot:%v", kv.me, msg.Command)
			kv.ApplySnapshotMsg(msg)
		}
	}
}

func (kv *KVServer) ApplyCommandMsg(msg raft.ApplyMsg) {
	if msg.CommandIndex <= kv.RaftlastIncludedIndex {
		return
	}
	op := msg.Command.(Op)

	var (
		keyexist bool
		value    string
		err      Err
	)
	defer func() {
		kv.mu.Lock()
		ch, channelexist := kv.WaitMap[msg.CommandIndex]
		if channelexist {
			DPrintf("[%d]: ApplyCommandMsg: %s: %s->'%s', %s'", kv.me, op.OpType, op.Key, value, err)
			DPrintf("[%d]: Applier send: %s->'%s'", kv.me, op.Key, value)
			kv.mu.Unlock()
			ch <- WaitMsg{Err: err, Op: op}
			return
		}
		kv.mu.Unlock()
	}()
	if op.OpType == "Get" {
		kv.mu.Lock()
		value, keyexist = kv.kvMap[op.Key]
		if !keyexist {
			err = ErrNoKey
		}
		DPrintf("[%d]: ApplyCommandMsg Do Get: %s->'%s'", kv.me, op.Key, value)
		kv.mu.Unlock()
	}
	kv.mu.Lock()
	if seq, exist := kv.seqMap[op.ClientID]; !exist || seq < op.Seq {
		if (op.OpType == "Put") || (op.OpType == "Append") {
			value = kv.putAppend(op)
			DPrintf("[%d]: ApplyCommandMsg Do putappend: %s: %s->'%s'", kv.me, op.OpType, op.Key, op.Value)
		}
		kv.seqMap[op.ClientID] = Max(kv.seqMap[op.ClientID], op.Seq)
	}
	kv.mu.Unlock()
	err = OK
	if kv.maxraftstate != -1 {
		kv.TryMakeSnapshot(msg.CommandIndex)
	}
}

func (kv *KVServer) putAppend(op Op) string {
	switch op.OpType {
	case "Put":
		DPrintf("[%d]: putAppend Put: %s", kv.me, op.Key)
		kv.kvMap[op.Key] = op.Value
		return kv.kvMap[op.Key]
	case "Append":
		DPrintf("[%d]: putAppend Append: %s", kv.me, op.Key)
		if _, exist := kv.kvMap[op.Key]; !exist {
			kv.kvMap[op.Key] = ""
		}
		kv.kvMap[op.Key] = strings.Join([]string{kv.kvMap[op.Key], op.Value}, "")
		return kv.kvMap[op.Key]
	}
	return ""
}

// Get snapshot from applyCh
func (kv *KVServer) ApplySnapshotMsg(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[%d]: ApplySnapshotMsg: Index:%d Term: %d", kv.me, msg.SnapshotIndex, msg.SnapshotTerm)
	if !kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		return
	}
	DPrintf("[%d]: ApplySnapshotMsg: InstallSnapshot", kv.me)
	kv.ReadSnapShot(msg.Snapshot)
	kv.RaftlastIncludedIndex = msg.SnapshotIndex
}

func (kv *KVServer) ReadSnapShot(data []byte) {
	if data == nil || len(data) < 1 {
		DPrintf("[%d]: ReadSnapShot: no snapshot", kv.me)
		return
	}
	var tmpkvMap map[string]string
	var tmpseqMap map[int64]int64
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&tmpkvMap) != nil || d.Decode(&tmpseqMap) != nil {
		DPrintf("[%d]: ReadSnapShot: decode error", kv.me)
		return
	}
	kv.kvMap = tmpkvMap
	kv.seqMap = tmpseqMap
	DPrintf("[%d]: ReadSnapShot: %v,%v", kv.me, kv.kvMap, kv.seqMap)
}

func (kv *KVServer) TryMakeSnapshot(raftIndex int) {
	if kv.rf.GetRaftStateSize() < kv.maxraftstate/10*proportion {
		return
	}
	kv.mu.Lock()
	DPrintf("[%d]: MakeSnapshot: %d,Map: %v", kv.me, kv.rf.GetRaftStateSize(), kv.kvMap)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.seqMap)
	data := w.Bytes()
	kv.mu.Unlock()
	kv.rf.Snapshot(raftIndex, data)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.WaitMap = make(map[int]chan WaitMsg)
	kv.seqMap = make(map[int64]int64)

	go kv.applier()
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ReadSnapShot(snapshot)
	}
	DPrintf("[%d]: StartKVServer", kv.me)

	return kv
}
