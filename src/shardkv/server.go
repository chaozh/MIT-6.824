package shardkv

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
	"6.824/shardctrler"
)

const Debug = true

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

type ShardComponent struct {
	ShardIndex   int
	KVDBofShard  map[string]string
	ClientSeq    map[int64]int64
	IsMigrateing bool
}

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
	kvDB       [shardctrler.NShards]ShardComponent
	kvNeedSend [shardctrler.NShards]bool // shard index -> group index

	WaitMap map[int]chan WaitMsg //key:index in raft, value:channel
}

type WaitMsg struct {
	Err Err
	Op  Op
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		DPrintf("[%d,%d,%d]: Get Wrong Leader: %s", kv.gid, kv.me, kv.config.Num, args.Key)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	if kv.config.Num < args.ConfigNum {
		DPrintf("[%d,%d,%d]: Get Wrong Config: %d,%d", kv.gid, kv.me, kv.config.Num, args.ConfigNum, kv.config.Num)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		DPrintf("[%d,%d,%d]: Get Wrong Group: %s,shade:%d", kv.gid, kv.me, kv.config.Num, args.Key, shard)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.kvDB[shard].IsMigrateing {
		DPrintf("[%d,%d,%d]: Get Shard no ready: %s,shade:%d", kv.gid, kv.me, kv.config.Num, args.Key, shard)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	kv.mu.Lock()
	op := Op{
		OpType:    "Get",
		Key:       args.Key,
		ClientID:  args.ClientID,
		Seq:       args.Seq,
		ConfigNum: args.ConfigNum,
	}
	logindex, _, _ := kv.rf.Start(op)
	kv.mu.Unlock()

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
			DPrintf("[%d,%d,%d]: Get Wrong RPC: %s, %s", kv.gid, kv.me, kv.config.Num, args.Key, waitmsg.Err)
			reply.Err = ErrWrongLeader
			return
		}
		if waitmsg.Err == OK {
			kv.mu.Lock()
			if kv.config.Shards[shard] != kv.gid {
				DPrintf("[%d,%d,%d]: Get Wrong Group: %s", kv.gid, kv.me, kv.config.Num, args.Key)
				reply.Err = ErrWrongGroup
				kv.mu.Unlock()
				return
			}
			if kv.kvDB[shard].IsMigrateing {
				DPrintf("[%d,%d,%d]: Get Shard no ready: %s", kv.gid, kv.me, kv.config.Num, args.Key)
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			if waitmsg.Op.ConfigNum != kv.config.Num {
				DPrintf("[%d,%d,%d]: Get Wrong Config: %d,%d", kv.gid, kv.me, kv.config.Num, args.ConfigNum, kv.config.Num)
				reply.Err = ErrWrongLeader
				kv.mu.Unlock()
				return
			}
			reply.Value = kv.kvDB[shard].KVDBofShard[args.Key]
			DPrintf("[%d,%d,%d]: Get RPC: %s->'%s'", kv.gid, kv.me, kv.config.Num, args.Key, reply.Value)
			kv.mu.Unlock()
			reply.Err = OK
		} else {
			reply.Err = waitmsg.Err
		}
		return
	case <-time.After(time.Duration(500) * time.Millisecond):
		DPrintf("[%d,%d,%d]: Get Timeout: %s", kv.gid, kv.me, kv.config.Num, args.Key)
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.

	_, ifLeader := kv.rf.GetState()
	if !ifLeader {
		DPrintf("[%d,%d,%d]: PutAppend Wrong Leader: %s", kv.gid, kv.me, kv.config.Num, args.Key)
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.config.Num < args.ConfigNum {
		DPrintf("[%d,%d,%d]: PutAppend Wrong Config: %d,kv.config.Num:%d", kv.gid, kv.me, kv.config.Num, args.ConfigNum, kv.config.Num)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		DPrintf("[%d,%d,%d]: PutAppend Wrong Group: %s", kv.gid, kv.me, kv.config.Num, args.Key)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.kvDB[shard].IsMigrateing {
		DPrintf("[%d,%d,%d]: Get Shard no ready: %s", kv.gid, kv.me, kv.config.Num, args.Key)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		OpType:    args.Op,
		Key:       args.Key,
		ClientID:  args.ClientID,
		Seq:       args.Seq,
		Value:     args.Value,
		ConfigNum: args.ConfigNum,
	}
	index, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	DPrintf("[%d,%d,%d]: PutAppend: %s: %s->'%s'", kv.gid, kv.me, kv.config.Num, args.Op, args.Key, args.Value)
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

	DPrintf("[%d,%d,%d]: PutAppend wait: %s: %s->'%s'", kv.gid, kv.me, kv.config.Num, args.Op, args.Key, args.Value)
	select {
	case waitmsg := <-chForIndex:
		if waitmsg.Op.Key != args.Key || waitmsg.Op.ClientID != args.ClientID || waitmsg.Op.Seq != args.Seq {
			DPrintf("[%d,%d,%d]: Put Append Wrong RPC: %s, %s", kv.gid, kv.me, kv.config.Num, args.Key, waitmsg.Err)
			reply.Err = ErrWrongLeader
			return
		}
		DPrintf("[%d,%d,%d]: Put Append %s RPC: %s->'%s', %s", kv.gid, kv.me, kv.config.Num, args.Op, args.Key, args.Value, waitmsg.Err)
		reply.Err = waitmsg.Err
		return
	case <-time.After(time.Duration(500) * time.Millisecond):
		DPrintf("[%d,%d,%d]: %s Timeout: %s", kv.gid, kv.me, kv.config.Num, args.Op, args.Key, args.Value)
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
	DPrintf("[%d,%d,%d]: Kill", kv.gid, kv.me)
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
}

func (kv *ShardKV) ApplyDBOp(op Op, raftindex int) {
	var (
		keyexist bool
		value    string
		err      Err
	)

	defer func() {
		kv.mu.Lock()
		ch, channelexist := kv.WaitMap[raftindex]
		if channelexist {
			DPrintf("[%d,%d,%d]: ApplyCommandMsg: %s: %s->'%s', %s'", kv.gid, kv.me, kv.config.Num, op.OpType, op.Key, value, err)
			DPrintf("[%d,%d,%d]: Applier send: %s->'%s'", kv.gid, kv.me, kv.config.Num, op.Key, value)
			kv.mu.Unlock()
			ch <- WaitMsg{Err: err, Op: op}
			return
		}
		kv.mu.Unlock()
	}()

	shard := key2shard(op.Key)

	if seq, exist := kv.kvDB[shard].ClientSeq[op.ClientID]; !exist || seq < op.Seq {
		switch op.OpType {
		case "Get":
			{
				kv.mu.Lock()
				value, keyexist = kv.kvDB[shard].KVDBofShard[op.Key]
				if !keyexist {
					err = ErrNoKey
					kv.mu.Unlock()
					return
				}
				DPrintf("[%d,%d,%d]: ApplyCommandMsg Do Get: %s->'%s'", kv.gid, kv.me, kv.config.Num, op.Key, value)
				kv.mu.Unlock()
			}

		case "Put":
			{
				kv.mu.Lock()
				value = kv.putAppend(op, shard)
				kv.mu.Unlock()
			}
		case "Append":
			{
				kv.mu.Lock()
				value = kv.putAppend(op, shard)
				kv.mu.Unlock()
			}
		}
		kv.mu.Lock()
		kv.kvDB[shard].ClientSeq[op.ClientID] = Max(kv.kvDB[shard].ClientSeq[op.ClientID], op.Seq)
		kv.mu.Unlock()
	}
	err = OK
	if kv.maxraftstate != -1 {
		kv.TryMakeSnapshot(raftindex, false)
	}
}

func (kv *ShardKV) putAppend(op Op, shard int) string {
	if kv.kvDB[shard].KVDBofShard == nil {
		kv.kvDB[shard].KVDBofShard = make(map[string]string)
	}
	switch op.OpType {
	case "Put":
		DPrintf("[%d,%d,%d]: putAppend Put: %s", kv.gid, kv.me, kv.config.Num, op.Key)
		kv.kvDB[shard].KVDBofShard[op.Key] = op.Value
		return kv.kvDB[shard].KVDBofShard[op.Key]
	case "Append":
		DPrintf("[%d,%d,%d]: putAppend Append: %s", kv.gid, kv.me, kv.config.Num, op.Key)
		if _, exist := kv.kvDB[shard].KVDBofShard[op.Key]; !exist {
			kv.kvDB[shard].KVDBofShard[op.Key] = ""
		}
		kv.kvDB[shard].KVDBofShard[op.Key] = strings.Join([]string{kv.kvDB[shard].KVDBofShard[op.Key], op.Value}, "")
		return kv.kvDB[shard].KVDBofShard[op.Key]
	}
	return ""
}

// Get snapshot from applyCh
func (kv *ShardKV) ApplySnapshotMsg(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if !kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		return
	}
	DPrintf("[%d,%d,%d]: ApplySnapshotMsg: InstallSnapshot", kv.gid, kv.me)
	kv.ReadSnapShot(msg.Snapshot)
	kv.RaftlastIncludedIndex = msg.SnapshotIndex
}

func (kv *ShardKV) ReadSnapShot(data []byte) {
	if data == nil || len(data) < 1 {
		DPrintf("[%d,%d,%d]: ReadSnapShot: no snapshot", kv.gid, kv.me)
		return
	}
	var tmpkvDB [shardctrler.NShards]ShardComponent
	var tmpConfig shardctrler.Config
	var tmpNeedSend [shardctrler.NShards]bool
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&tmpkvDB) != nil ||
		d.Decode(&tmpNeedSend) != nil ||
		d.Decode(&tmpConfig) != nil {
		DPrintf("[%d,%d,%d]: ReadSnapShot: decode error", kv.gid, kv.me)
		return
	}
	kv.kvDB = tmpkvDB
	kv.config = tmpConfig
	kv.kvNeedSend = tmpNeedSend
	DPrintf("[%d,%d,%d]: ReadSnapShot: %v,%v", kv.gid, kv.me, kv.config.Num, kv.kvDB)
}

func (kv *ShardKV) TryMakeSnapshot(raftIndex int, force bool) {
	if !force && kv.rf.GetRaftStateSize() < kv.maxraftstate/10*proportion {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvDB)
	e.Encode(kv.kvNeedSend)
	e.Encode(kv.config)
	data := w.Bytes()
	kv.mu.Unlock()
	kv.rf.Snapshot(raftIndex, data)
	kv.mu.Lock()
}

func (kv *ShardKV) ticker() {
	for !kv.killed() {
		if _, isleader := kv.rf.GetState(); !isleader {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		kv.checkconfig()
		kv.checkShadeNeedPush()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) checkconfig() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// for _, shardinfo := range kv.kvDB {
	// 	if shardinfo.IsMigrateing {
	// 		return
	// 	}
	// }
	newcfg := kv.mck.Query(kv.config.Num + 1)
	if newcfg.Num <= kv.config.Num {
		return
	}
	DPrintf("[%d,%d,%d]: checknewconfig: %v", kv.gid, kv.me, kv.config.Num, newcfg)
	kv.rf.Start(ConfigOp{
		Config: newcfg,
	})
}

func (kv *ShardKV) ApplyConfigOp(op ConfigOp, raftindex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[%d,%d,%d]: ApplyConfigOp: %v", kv.gid, kv.me, kv.config.Num, op)
	if op.Config.Num <= kv.config.Num {
		return
	}
	oldcfg := kv.config
	kv.config = op.Config
	kv.checkShadeNeedMigrate(oldcfg)
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
			ShardIndex:   shard,
			KVDBofShard:  make(map[string]string),
			ClientSeq:    make(map[int64]int64),
			IsMigrateing: false,
		}
	}

	go kv.applier()
	go kv.ticker()
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ReadSnapShot(snapshot)
	}
	DPrintf("[%d,%d,%d]: Startshardkv", kv.gid, kv.me)

	return kv
}
