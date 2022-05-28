package shardkv

import (
	"sync/atomic"
	"time"

	"6.824/shardctrler"
)

const (
	PutShard      = "PutShard"
	PushShard     = "PushShard"
	GCShard       = "GCShard"
	ValidateShard = "ValidateShard"
)

const (
	invalid = iota
	valid
	migrating
	waitMigrate
	pushing
)

type Shardstate int

func (s Shardstate) String() string {
	switch s {
	case invalid:
		return "invalid"
	case valid:
		return "valid"
	case migrating:
		return "migrating"
	case waitMigrate:
		return "waitMigrate"
	case pushing:
		return "pushing"
	}
	return "unknown"
}

type ShardComponent struct {
	ShardIndex  int
	KVDBofShard map[string]string
	ClientSeq   map[int64]int64
	State       Shardstate
}

type ShardOp struct {
	Optype    string
	Shard     ShardComponent
	Group     int
	Confignum int
}

func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {
	kv.mu.Lock()
	_, isleader := kv.rf.GetState()
	reply.ConfigNum = kv.config.Num
	if !isleader {
		DPrintf("[%d,%d,%d]: PutShard Wrong Leader: %d", kv.gid, kv.me, kv.config.Num, args.Shard.ShardIndex)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	// if kv.config.Num < args.ConfigNum {
	// 	DPrintf("[%d,%d,%d]: PutShard Wrong Config: %d", kv.gid, kv.me, kv.config.Num, args.Shard.ShardIndex)
	// 	reply.Err = ErrWrongLeader
	// 	kv.mu.Unlock()
	// 	return
	// }
	// if kv.config.Shards[args.Shard.ShardIndex] != kv.gid || kv.kvDB[args.Shard.ShardIndex].State == invalid {
	// 	DPrintf("[%d,%d,%d]: PutShard Wrong Shard: %d", kv.gid, kv.me, kv.config.Num, args.Shard.ShardIndex)
	// 	reply.Err = ErrWrongGroup
	// 	kv.mu.Unlock()
	// 	return
	// }
	if kv.kvDB[args.Shard.ShardIndex].State == migrating ||
		kv.kvDB[args.Shard.ShardIndex].State == valid {
		DPrintf("[%d,%d,%d]: PutShard Already Migrate: %d", kv.gid, kv.me, kv.config.Num, args.Shard.ShardIndex)
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	//state is waitMigrate
	sop := ShardOp{
		Optype:    PutShard,
		Shard:     args.Shard,
		Confignum: args.ConfigNum,
	}
	kv.mu.Unlock()
	logindex, _, _ := kv.rf.Start(sop)

	kv.mu.Lock()
	DPrintf("[%d,%d,%d]: PutShard Start: %d,raftindex:%d", kv.gid, kv.me, kv.config.Num, args.Shard.ShardIndex, logindex)
	chForIndex, exist := kv.WaitMap[logindex]
	if !exist {
		DPrintf("[%d,%d,%d]: PutShard WaitMap: %d", kv.gid, kv.me, kv.config.Num, args.Shard.ShardIndex)
		kv.WaitMap[logindex] = make(chan WaitMsg, 1)
		chForIndex = kv.WaitMap[logindex]
	}
	defer func() {
		kv.mu.Lock()
		delete(kv.WaitMap, logindex)
		kv.mu.Unlock()
	}()
	kv.mu.Unlock()

	select {
	case msg := <-chForIndex:
		reply.Err = msg.Err
		return
	case <-time.After(time.Duration(3000) * time.Millisecond):
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) pushShard(op ShardOp) {
	kv.mu.Lock()
	if _, isleader := kv.rf.GetState(); !isleader {
		kv.mu.Unlock()
		return
	}
	Shardindex := op.Shard.ShardIndex
	group := op.Group
	Shard := kv.kvDB[Shardindex]

	if op.Confignum != kv.config.Num {
		DPrintf("[%d,%d,%d]: pushShard out of date config: %d", kv.gid, kv.me, kv.config.Num, op.Confignum)
		kv.mu.Unlock()
		return
	}
	if group == kv.gid {
		DPrintf("[%d,%d,%d]: pushShard same group: %d,pushing:%d", kv.gid, kv.me, kv.config.Num, Shard.ShardIndex, kv.pushingshard)
		kv.mu.Unlock()
		return
	}
	if kv.kvDB[Shardindex].State != migrating {
		DPrintf("[%d,%d,%d]: pushShard already done: %d,pushing:%d", kv.gid, kv.me, kv.config.Num, Shard.ShardIndex, kv.pushingshard)
		kv.mu.Unlock()
		return
	}

	atomic.AddInt64(&kv.pushingshard, 1)
	defer func() {
		kv.mu.Lock()
		atomic.AddInt64(&kv.pushingshard, -1)
		kv.mu.Unlock()
	}()
	DPrintf("[%d,%d,%d]: pushShard: %d", kv.gid, kv.me, kv.config.Num, Shard.ShardIndex)
	args := PushShardArgs{
		Shard:     Shard,
		ConfigNum: kv.config.Num,
	}
	if servers, ok := kv.config.Groups[group]; ok {
		for i := 0; i < len(servers); i++ {
			srv := kv.make_end(servers[i])
			reply := PushShardReply{}
			kv.mu.Unlock()
			ok := srv.Call("ShardKV.PushShard", &args, &reply) //don't unlock
			kv.mu.Lock()
			if ok && reply.Err == OK {
				DPrintf("[%d,%d,%d]: pushShard done: %d->%d", kv.gid, kv.me, kv.config.Num, Shard.ShardIndex, group)
				sop := ShardOp{
					Optype:    GCShard,
					Shard:     Shard,
					Confignum: kv.config.Num,
				}
				kv.mu.Unlock()
				kv.rf.Start(sop)
				return
			}
			if ok && reply.Err == ErrConfigOutOfDate {
				DPrintf("[%d,%d,%d]: pushShard out of date: %d", kv.gid, kv.me, kv.config.Num, Shard.ShardIndex)
				kv.mu.Unlock()
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				DPrintf("[%d,%d,%d]: pushShard wrong group: %d", kv.gid, kv.me, kv.config.Num, Shard.ShardIndex)
				kv.mu.Unlock()
				return
			}
			// wrong leader or timeout
			DPrintf("[%d,%d,%d]: pushShard to %d:%d failed: %d,%s", kv.gid, kv.me, kv.config.Num, group, i, Shard.ShardIndex, reply.Err)
		}
	}
	kv.mu.Unlock()
	DPrintf("[%d,%d,%d]: pushShard to group %d failed: %d", kv.gid, kv.me, kv.config.Num, group, Shard.ShardIndex)
}

func (kv *ShardKV) ApplyShardOp(op ShardOp, raftindex int) {
	var err Err
	defer func() {
		kv.mu.Lock()
		ch, channelexist := kv.WaitMap[raftindex]
		kv.mu.Unlock()
		if channelexist {
			ch <- WaitMsg{Err: err}
		}
	}()

	switch op.Optype {
	case PutShard:
		kv.mu.Lock()
		DPrintf("[%d,%d,%d]: ApplyShardOp: %d,raftIndex:%d", kv.gid, kv.me, kv.config.Num, op.Shard.ShardIndex, raftindex)
		if kv.kvDB[op.Shard.ShardIndex].State != waitMigrate {
			if kv.config.Num > op.Confignum {
				DPrintf("[%d,%d,%d]: ApplyShardOp Out of Date Config: %d", kv.gid, kv.me, kv.config.Num, op.Shard.ShardIndex)
				err = OK
				kv.mu.Unlock()
				return
			}
			DPrintf("[%d,%d,%d]: ApplyShardOp Shard not wait migrate: %d,%s", kv.gid, kv.me, kv.config.Num, op.Shard.ShardIndex, kv.kvDB[op.Shard.ShardIndex].State)
			err = ErrWrongLeader
			kv.mu.Unlock()
			break
		}
		kv.kvDB[op.Shard.ShardIndex] = op.Shard
		if kv.config.Shards[op.Shard.ShardIndex] != kv.gid {
			kv.kvDB[op.Shard.ShardIndex].State = migrating
		} else {
			kv.kvDB[op.Shard.ShardIndex].State = valid
		}
		DPrintf("[%d,%d,%d]: ApplyShardOp done: %d,raftIndex:%d,Shard:%v", kv.gid, kv.me, kv.config.Num, op.Shard.ShardIndex, raftindex, kv.kvDB[op.Shard.ShardIndex])
		err = OK
		kv.mu.Unlock()
	case GCShard:
		kv.gcShard(op)
	case ValidateShard:
		kv.validateShard(op)
	case PushShard:
		kv.pushShard(op)
	}
}

func (kv *ShardKV) validateShard(op ShardOp) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch kv.kvDB[op.Shard.ShardIndex].State {
	case migrating:
		DPrintf("[%d,%d,%d]: validateShard: %d,%s", kv.gid, kv.me, kv.config.Num, op.Shard.ShardIndex, kv.kvDB[op.Shard.ShardIndex].State)
		kv.kvDB[op.Shard.ShardIndex].State = valid
	case invalid:
		DPrintf("[%d,%d,%d]: validateShard: %d,%s", kv.gid, kv.me, kv.config.Num, op.Shard.ShardIndex, kv.kvDB[op.Shard.ShardIndex].State)
		kv.kvDB[op.Shard.ShardIndex].State = waitMigrate
	}
}

func (kv *ShardKV) gcShard(op ShardOp) {
	kv.mu.Lock()
	if kv.kvDB[op.Shard.ShardIndex].State != migrating {
		DPrintf("[%d,%d,%d]: shardGCOp Shard not migrating: %d,%s", kv.gid, kv.me, kv.config.Num, op.Shard.ShardIndex, kv.kvDB[op.Shard.ShardIndex].State)
		kv.mu.Unlock()
		return
	}
	kv.kvDB[op.Shard.ShardIndex] = ShardComponent{
		ShardIndex:  op.Shard.ShardIndex,
		KVDBofShard: nil,
		ClientSeq:   nil,
		State:       invalid,
	}
	DPrintf("[%d,%d,%d]: shardGCOp done: %d", kv.gid, kv.me, kv.config.Num, op.Shard.ShardIndex)
	kv.mu.Unlock()
}

func (kv *ShardKV) checkShardMigrate(oldcfg shardctrler.Config) {
	for shard := range kv.config.Shards {
		if kv.config.Shards[shard] == oldcfg.Shards[shard] {
			continue
		}
		if kv.config.Shards[shard] == kv.gid {
			if oldcfg.Shards[shard] == 0 && kv.config.Num == 1 {
				kv.kvDB[shard].State = valid
				continue
			}
			switch kv.kvDB[shard].State {
			case invalid:
				DPrintf("[%d,%d,%d]: checkShardMigrate: %d,%s", kv.gid, kv.me, kv.config.Num, shard, kv.kvDB[shard].State)
				kv.kvDB[shard].State = waitMigrate
				// case migrating:
				// 	DPrintf("[%d,%d,%d]: checkShardMigrate migrating to vaild: %d,%s", kv.gid, kv.me, kv.config.Num, shard, kv.kvDB[shard].State)
				// 	kv.kvDB[shard].State = valid
			}
			continue
		}
		if oldcfg.Shards[shard] == kv.gid {
			switch kv.kvDB[shard].State {
			// case waitMigrate:
			// 	DPrintf("[%d,%d,%d]: checkShardMigrate: %d,%s", kv.gid, kv.me, kv.config.Num, shard, kv.kvDB[shard].State)
			// 	kv.kvDB[shard].State = invalid
			case valid:
				DPrintf("[%d,%d,%d]: checkShardMigrate: %d,%s", kv.gid, kv.me, kv.config.Num, shard, kv.kvDB[shard].State)
				kv.kvDB[shard].State = migrating
			}
		}
	}
}

func (kv *ShardKV) checkShardNeedPush() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for index, compoment := range kv.kvDB {
		if compoment.State != migrating {
			continue
		}
		DPrintf("[%d,%d,%d]: checkShardNeedPush: %d->%d", kv.gid, kv.me, kv.config.Num, index, kv.config.Shards[index])
		sop := ShardOp{
			Optype: "PushShard",
			Shard: ShardComponent{
				ShardIndex: index,
			},
			Group:     kv.config.Shards[index],
			Confignum: kv.config.Num,
		}
		// kv.mu.Unlock()
		// ridx, _, _ := kv.rf.Start(sop)
		// kv.mu.Lock()
		go kv.pushShard(sop)
	}
}
