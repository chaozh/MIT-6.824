package shardkv

import (
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = false

const proportion = 9

func DPrintf(format string, a ...interface{}) (n int, err error) {
	str := strings.Join([]string{"[shardkv]:", format}, "")
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
	OpType string
	Key    string
	Value  string

	ClientID  int64
	Seq       int64
	ConfigNum int
}

type ConfigOp struct {
	Config shardctrler.Config
}

const (
	Invaild = iota
	Vaild
	ReadyPush
)

type ShardKV struct {
	mu                    sync.Mutex
	me                    int
	rf                    *raft.Raft
	applyCh               chan raft.ApplyMsg
	dead                  int32 // set by Kill()
	make_end              func(string) *labrpc.ClientEnd
	gid                   int
	ctrlers               []*labrpc.ClientEnd
	maxraftstate          int // snapshot if log grows this big
	RaftlastIncludedIndex int
	mck                   *shardctrler.Clerk
	config                shardctrler.Config

	// Your definitions here.
	kvDB [shardctrler.NShards]ShardComponent

	WaitMap map[int]chan WaitMsg //key:index in raft, value:channel

	pushingshard int64
}

type WaitMsg struct {
	Err Err
	Op  Op
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	kv.mu.Lock()
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		DPrintf("[%d,%d,%d]: Get Wrong Leader: %s", kv.gid, kv.me, kv.config.Num, args.Key)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}

	shard := key2shard(args.Key)
	// state == vaild

	op := Op{
		OpType:    "Get",
		Key:       args.Key,
		ClientID:  args.ClientID,
		Seq:       args.Seq,
		ConfigNum: kv.config.Num,
	}
	kv.mu.Unlock()
	logindex, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	DPrintf("[%d,%d,%d]: Get: %s", kv.gid, kv.me, kv.config.Num, args.Key)
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

	DPrintf("[%d,%d,%d]: Get wait: %s", kv.gid, kv.me, kv.config.Num, args.Key)
	select {
	case waitmsg := <-chForIndex:
		if waitmsg.Op.Key != args.Key || waitmsg.Op.ClientID != args.ClientID || waitmsg.Op.Seq != args.Seq {
			reply.Err = ErrWrongLeader
			return
		}
		if waitmsg.Err == OK {
			kv.mu.Lock()
			reply.Value = kv.kvDB[shard].KVDBofShard[args.Key]
			kv.mu.Unlock()
			reply.Err = OK
		} else {
			reply.Err = waitmsg.Err
		}
		return
	case <-time.After(time.Duration(500) * time.Millisecond):
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	kv.mu.Lock()
	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		DPrintf("[%d,%d,%d]: PutAppend Wrong Leader: %s", kv.gid, kv.me, kv.config.Num, args.Key)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	// state == vaild

	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		ClientID:  args.ClientID,
		Seq:       args.Seq,
		Value:     args.Value,
		ConfigNum: kv.config.Num,
	}
	kv.mu.Unlock()
	index, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	DPrintf("[%d,%d,%d]: PutAppend: %s: %s->'%s'", kv.gid, kv.me, kv.config.Num, args.Op, args.Key, args.Value)
	chForIndex, exist := kv.WaitMap[index]
	if !exist {
		kv.WaitMap[index] = make(chan WaitMsg, 1)
		chForIndex = kv.WaitMap[index]
	}
	defer func() {
		kv.mu.Lock()
		delete(kv.WaitMap, index)
		kv.mu.Unlock()
	}()

	DPrintf("[%d,%d,%d]: PutAppend wait: %s: %s->'%s'", kv.gid, kv.me, kv.config.Num, args.Op, args.Key, args.Value)
	kv.mu.Unlock()
	select {
	case waitmsg := <-chForIndex:
		if waitmsg.Op.Key != args.Key || waitmsg.Op.ClientID != args.ClientID || waitmsg.Op.Seq != args.Seq {
			reply.Err = ErrWrongLeader
			return
		}
		reply.Err = waitmsg.Err
		return
	case <-time.After(time.Duration(500) * time.Millisecond):
		reply.Err = ErrWrongLeader
		return
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	atomic.StoreInt32(&kv.dead, 1)
	DPrintf("[%d,%d]: Kill", kv.gid, kv.me)
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			kv.ApplyCommandMsg(msg)
		} else if msg.SnapshotValid {
			kv.ApplySnapshotMsg(msg)
		}
	}
}

func (kv *ShardKV) ApplyCommandMsg(msg raft.ApplyMsg) {
	if msg.CommandIndex <= kv.RaftlastIncludedIndex {
		DPrintf("[%d,%d,%d]: ApplyCommandMsg less than RaftlastIncludedIndex: %d->%d,%v", kv.gid, kv.me, kv.config.Num, kv.RaftlastIncludedIndex, msg.CommandIndex, msg.Command)
		return
	}
	switch msg.Command.(type) {
	case Op:
		op := msg.Command.(Op)
		kv.ApplyDBOp(op, msg.CommandIndex)
	case ConfigOp:
		cop := msg.Command.(ConfigOp)
		kv.ApplyConfigOp(cop, msg.CommandIndex)
	case ShardOp:
		sop := msg.Command.(ShardOp)
		kv.ApplyShardOp(sop, msg.CommandIndex)
	}
	kv.TryMakeSnapshot(msg.CommandIndex, false)
}

func (kv *ShardKV) ApplyDBOp(op Op, raftindex int) {
	var (
		keyexist bool
		value    string
		err      Err
	)

	shard := key2shard(op.Key)
	defer func() {
		kv.mu.Lock()
		ch, channelexist := kv.WaitMap[raftindex]
		if channelexist {
			DPrintf("[%d,%d,%d]: ApplyCommandMsg: %s: %s->'%s', %s'", kv.gid, kv.me, kv.config.Num, op.OpType, op.Key, value, err)
			DPrintf("[%d,%d,%d]: Applier send: %s->'%s'", kv.gid, kv.me, kv.config.Num, op.Key, value)
			ch <- WaitMsg{Err: err, Op: op}
		}
		kv.mu.Unlock()
	}()

	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Shards[shard] != kv.gid ||
		kv.kvDB[shard].State == invalid ||
		kv.kvDB[shard].State == migrating {
		DPrintf("[%d,%d,%d]: Get Wrong Group: %s,shade:%d", kv.gid, kv.me, kv.config.Num, op.Key, shard)
		err = ErrWrongGroup
		return
	}
	if kv.kvDB[shard].State == waitMigrate {
		DPrintf("[%d,%d,%d]: Get Shard no ready: %s,shade:%d", kv.gid, kv.me, kv.config.Num, op.Key, shard)
		err = ErrWrongLeader
		return
	}
	switch op.OpType {
	case "Get":
		{
			value, keyexist = kv.kvDB[shard].KVDBofShard[op.Key]
			DPrintf("[%d,%d,%d]: ApplyCommandMsg Do Get: %s->'%s'", kv.gid, kv.me, kv.config.Num, op.Key, value)
			if !keyexist {
				err = ErrNoKey
				break
			}
			err = OK
		}
	case "Put":
		fallthrough
	case "Append":
		if seq, exist := kv.kvDB[shard].ClientSeq[op.ClientID]; exist && seq >= op.Seq {
			err = OK
			break
		}
		DPrintf("[%d,%d,%d]: ApplyCommandMsg Do %s: %s->'%s'", kv.gid, kv.me, kv.config.Num, op.OpType, op.Key, op.Value)
		if kv.config.Shards[shard] != kv.gid ||
			kv.kvDB[shard].State == invalid ||
			kv.kvDB[shard].State == migrating {
			DPrintf("[%d,%d,%d]: ApplyCommandMsg Wrong State of shard: %s,%s", kv.gid, kv.me, kv.config.Num, op.Key, kv.kvDB[shard].State)
			err = ErrWrongGroup
			break
		}
		if kv.kvDB[shard].State == waitMigrate {
			err = ErrWrongLeader
			break
		}
		value = kv.putAppend(op, shard)
		err = OK
		kv.kvDB[shard].ClientSeq[op.ClientID] = Max(kv.kvDB[shard].ClientSeq[op.ClientID], op.Seq)
	}
}

func (kv *ShardKV) putAppend(op Op, shard int) string {
	if kv.kvDB[shard].KVDBofShard == nil {
		kv.kvDB[shard].KVDBofShard = make(map[string]string)
	}
	switch op.OpType {
	case "Put":
		kv.kvDB[shard].KVDBofShard[op.Key] = op.Value
		DPrintf("[%d,%d,%d]: putAppend Put: %s->%s", kv.gid, kv.me, kv.config.Num, op.Key, kv.kvDB[shard].KVDBofShard[op.Key])
		return kv.kvDB[shard].KVDBofShard[op.Key]
	case "Append":
		if _, exist := kv.kvDB[shard].KVDBofShard[op.Key]; !exist {
			kv.kvDB[shard].KVDBofShard[op.Key] = ""
		}
		kv.kvDB[shard].KVDBofShard[op.Key] = strings.Join([]string{kv.kvDB[shard].KVDBofShard[op.Key], op.Value}, "")
		DPrintf("[%d,%d,%d]: putAppend Append: %s->%s", kv.gid, kv.me, kv.config.Num, op.Key, kv.kvDB[shard].KVDBofShard[op.Key])
		return kv.kvDB[shard].KVDBofShard[op.Key]
	}
	return ""
}

// Get snapshot from applyCh

func (kv *ShardKV) ticker() {
	for !kv.killed() {
		if _, isleader := kv.rf.GetState(); !isleader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.checkconfig()
		kv.checkShardNeedPush()
		time.Sleep(100 * time.Millisecond)
	}
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(ShardOp{})
	labgob.Register(ConfigOp{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.WaitMap = make(map[int]chan WaitMsg)

	for shard := range kv.kvDB {
		kv.kvDB[shard] = ShardComponent{
			ShardIndex:  shard,
			KVDBofShard: make(map[string]string),
			ClientSeq:   make(map[int64]int64),
			State:       invalid,
		}
	}

	go kv.applier()
	go kv.ticker()
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ReadSnapShot(snapshot)
	}
	DPrintf("[%d,%d,%d]: Startshardkv", kv.gid, kv.me, kv.config.Num)

	return kv
}
