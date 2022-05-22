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
	OpType    string
	Key       string
	Value     string
	ConfigNum int
	Shard     int
	ShardMap  map[string]string

	ClientID int64
	Seq      int64
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
	kvMap      [shardctrler.NShards]map[string]string //DB
	validshard [shardctrler.NShards]bool
	WaitMap    map[int]chan WaitMsg //key:index in raft, value:channel
	seqMap     map[int64]int64
}

type WaitMsg struct {
	Err Err
	Op  Op
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		DPrintf("[%d,%d]: Get Wrong Group: %s", kv.gid, kv.me, args.Key)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

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
	DPrintf("[%d,%d]: Get: %s", kv.gid, kv.me, args.Key)
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

	DPrintf("[%d,%d]: Get wait: %s", kv.gid, kv.me, args.Key)
	select {
	case waitmsg := <-chForIndex:
		if waitmsg.Op.Key != args.Key || waitmsg.Op.ClientID != args.ClientID || waitmsg.Op.Seq != args.Seq {
			DPrintf("[%d,%d]: Get Wrong RPC: %s, %s", kv.gid, kv.me, args.Key, waitmsg.Err)
			reply.Err = ErrWrongLeader
			return
		}
		if waitmsg.Err == OK {
			kv.mu.Lock()
			if kv.config.Shards[shard] != kv.gid {
				DPrintf("[%d,%d]: Get Wrong Group: %s", kv.gid, kv.me, args.Key)
				reply.Err = ErrWrongGroup
				kv.mu.Unlock()
				return
			}
			if !kv.validshard[shard] {
				DPrintf("[%d,%d]: Get Wrong Shard: %s", kv.gid, kv.me, args.Key)
				reply.Err = ErrShardNoValid
				kv.mu.Unlock()
				return
			}
			reply.Value = kv.kvMap[shard][args.Key]
			DPrintf("[%d,%d]: Get RPC: %s->'%s'", kv.gid, kv.me, args.Key, reply.Value)
			kv.mu.Unlock()
			reply.Err = OK
		} else {
			reply.Err = waitmsg.Err
		}
		return
	case <-time.After(time.Duration(500) * time.Millisecond):
		DPrintf("[%d,%d]: Get Timeout: %s", kv.gid, kv.me, args.Key)
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.config.Shards[shard] != kv.gid {
		DPrintf("[%d,%d]: PutAppend Wrong Group: %s", kv.gid, kv.me, args.Key)
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

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
	DPrintf("[%d,%d]: PutAppend: %s: %s->'%s'", kv.gid, kv.me, args.Op, args.Key, args.Value)
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

	DPrintf("[%d,%d]: PutAppend wait: %s: %s->'%s'", kv.gid, kv.me, args.Op, args.Key, args.Value)
	select {
	case waitmsg := <-chForIndex:
		if waitmsg.Op.Key != args.Key || waitmsg.Op.ClientID != args.ClientID || waitmsg.Op.Seq != args.Seq {
			DPrintf("[%d,%d]: Put Append Wrong RPC: %s, %s", kv.gid, kv.me, args.Key, waitmsg.Err)
			reply.Err = ErrWrongLeader
			return
		}
		DPrintf("[%d,%d]: Put Append %s RPC: %s->'%s', %s", kv.gid, kv.me, args.Op, args.Key, args.Value, waitmsg.Err)
		reply.Err = waitmsg.Err
		return
	case <-time.After(time.Duration(500) * time.Millisecond):
		DPrintf("[%d,%d]: %s Timeout: %s", kv.gid, kv.me, args.Op, args.Key, args.Value)
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
		return
	}
	op := msg.Command.(Op)

	var (
		keyexist bool
		value    string
		err      Err
	)

	if op.OpType == "Config" {
		kv.mu.Lock()
		kv.pullnewconfig(op.ConfigNum)
		kv.mu.Unlock()
		return
	}

	if op.OpType == "Shard" {
		DPrintf("[%d,%d]: ApplyCommandMsg Shard: %d->%d", kv.gid, kv.me, kv.config.Shards[op.Shard])
		kv.mu.Lock()
		kv.kvMap[op.Shard] = op.ShardMap
		kv.validshard[op.Shard] = true
		kv.mu.Unlock()
		return
	}

	defer func() {
		kv.mu.Lock()
		ch, channelexist := kv.WaitMap[msg.CommandIndex]
		if channelexist {
			DPrintf("[%d,%d]: ApplyCommandMsg: %s: %s->'%s', %s'", kv.gid, kv.me, op.OpType, op.Key, value, err)
			DPrintf("[%d,%d]: Applier send: %s->'%s'", kv.gid, kv.me, op.Key, value)
			kv.mu.Unlock()
			ch <- WaitMsg{Err: err, Op: op}
			return
		}
		kv.mu.Unlock()
	}()

	shard := key2shard(op.Key)

	if op.OpType == "Get" {
		kv.mu.Lock()
		value, keyexist = kv.kvMap[shard][op.Key]
		if !keyexist {
			err = ErrNoKey
		}
		DPrintf("[%d,%d]: ApplyCommandMsg Do Get: %s->'%s'", kv.gid, kv.me, op.Key, value)
		kv.mu.Unlock()
	}

	kv.mu.Lock()
	if seq, exist := kv.seqMap[op.ClientID]; !exist || seq < op.Seq {
		if (op.OpType == "Put") || (op.OpType == "Append") {
			value = kv.putAppend(op, shard)
			DPrintf("[%d,%d]: ApplyCommandMsg Do putappend: %s: %s->'%s'", kv.gid, kv.me, op.OpType, op.Key, op.Value)
		}
		kv.seqMap[op.ClientID] = Max(kv.seqMap[op.ClientID], op.Seq)
	}
	kv.mu.Unlock()
	err = OK
	if kv.maxraftstate != -1 {
		kv.TryMakeSnapshot(msg.CommandIndex)
	}
}

func (kv *ShardKV) putAppend(op Op, shard int) string {
	if kv.kvMap[shard] == nil {
		kv.kvMap[shard] = make(map[string]string)
	}
	switch op.OpType {
	case "Put":
		DPrintf("[%d,%d]: putAppend Put: %s", kv.gid, kv.me, op.Key)
		kv.kvMap[shard][op.Key] = op.Value
		return kv.kvMap[shard][op.Key]
	case "Append":
		DPrintf("[%d,%d]: putAppend Append: %s", kv.gid, kv.me, op.Key)
		if _, exist := kv.kvMap[shard][op.Key]; !exist {
			kv.kvMap[shard][op.Key] = ""
		}
		kv.kvMap[shard][op.Key] = strings.Join([]string{kv.kvMap[shard][op.Key], op.Value}, "")
		return kv.kvMap[shard][op.Key]
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
	DPrintf("[%d,%d]: ApplySnapshotMsg: InstallSnapshot", kv.gid, kv.me)
	kv.ReadSnapShot(msg.Snapshot)
	kv.RaftlastIncludedIndex = msg.SnapshotIndex
}

func (kv *ShardKV) ReadSnapShot(data []byte) {
	if data == nil || len(data) < 1 {
		DPrintf("[%d,%d]: ReadSnapShot: no snapshot", kv.gid, kv.me)
		return
	}
	var tmpkvMap [shardctrler.NShards]map[string]string
	var tmpseqMap map[int64]int64
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&tmpkvMap) != nil || d.Decode(&tmpseqMap) != nil {
		DPrintf("[%d,%d]: ReadSnapShot: decode error", kv.gid, kv.me)
		return
	}
	kv.kvMap = tmpkvMap
	kv.seqMap = tmpseqMap
	DPrintf("[%d,%d]: ReadSnapShot: %v,%v", kv.gid, kv.me, kv.kvMap, kv.seqMap)
}

func (kv *ShardKV) TryMakeSnapshot(raftIndex int) {
	if kv.rf.GetRaftStateSize() < kv.maxraftstate/10*proportion {
		return
	}
	kv.mu.Lock()
	DPrintf("[%d,%d]: MakeSnapshot: %d,Map: %v", kv.gid, kv.me, kv.rf.GetRaftStateSize(), kv.kvMap)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvMap)
	e.Encode(kv.seqMap)
	data := w.Bytes()
	kv.mu.Unlock()
	kv.rf.Snapshot(raftIndex, data)
}

func (kv *ShardKV) GetShard(args *ShardArg, reply *ShardReply) {
	// if args.ConfigNum < kv.config.Num{
	// 	reply.Err =
	// }
	kv.mu.Lock()
	if _, isleader := kv.rf.GetState(); !isleader {
		DPrintf("[%d,%d]: Get Shard: Not Leader", kv.gid, kv.me)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if args.ConfigNum > kv.config.Num {
		kv.checkconfig(args.ConfigNum)
	}
	DPrintf("[%d,%d]: Get Shard: %d,%d", kv.gid, kv.me, args.ConfigNum, args.Shard)
	reply.Err = OK
	reply.ShardMap = kv.kvMap[args.Shard]
	reply.SeqMap = kv.seqMap
	kv.mu.Unlock()
}

func (kv *ShardKV) pullnewshard(oldcfg shardctrler.Config, oldgroup int, shard int) {
	if oldgroup == 0 {
		kv.validshard[shard] = true
		return
	}
	DPrintf("[%d,%d]: pullnewshard: %d,%d", kv.gid, kv.me, oldgroup, shard)
	args := ShardArg{
		Shard:     shard,
		ConfigNum: kv.config.Num,
	}
	if servers, ok := oldcfg.Groups[oldgroup]; ok {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var reply ShardReply
			kv.mu.Unlock()
			ok := srv.Call("ShardKV.GetShard", &args, &reply)
			kv.mu.Lock()
			if ok && reply.Err == OK {
				DPrintf("[%d,%d]: pullnewshard done: %d->%v", kv.gid, kv.me, shard, reply.ShardMap)
				kv.rf.Start(Op{
					OpType:   "Shard",
					Shard:    shard,
					ShardMap: reply.ShardMap,
				})
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				break
			}
		}
	}
}

func (kv *ShardKV) ticker() {
	for !kv.killed() {
		if _, isleader := kv.rf.GetState(); isleader {
			kv.mu.Lock()
			kv.checkconfig(-1)
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) checkconfig(num int) {
	if num < -1 {
		panic("checkconfig: num < -1")
	}
	newcfg := kv.mck.Query(num)
	if newcfg.Num <= kv.config.Num {
		return
	}
	DPrintf("[%d,%d]: checknewconfig: %v", kv.gid, kv.me, newcfg)
	kv.rf.Start(Op{OpType: "Config", ConfigNum: newcfg.Num})
}

func (kv *ShardKV) pullnewconfig(num int) bool {
	newcfg := kv.mck.Query(num)
	if newcfg.Num <= kv.config.Num {
		return false
	}
	DPrintf("[%d,%d]: pullnewconfig: %v", kv.gid, kv.me, newcfg)
	oldcfg := kv.config
	kv.config = newcfg
	kv.checknewshard(oldcfg, newcfg)
	return true
}

func (kv *ShardKV) checknewshard(oldcfg shardctrler.Config, newcfg shardctrler.Config) {
	for shard := range newcfg.Shards {
		if newcfg.Shards[shard] == oldcfg.Shards[shard] {
			continue
		}
		if newcfg.Shards[shard] == kv.gid {
			kv.pullnewshard(oldcfg, oldcfg.Shards[shard], shard)
		}
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
	kv.seqMap = make(map[int64]int64)

	go kv.applier()
	go kv.ticker()
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.ReadSnapShot(snapshot)
	}
	DPrintf("[%d,%d]: Startshardkv", kv.gid, kv.me)

	return kv
}
