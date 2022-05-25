package shardkv

import (
	"time"

	"6.824/shardctrler"
)

const (
	PutShard  = "PutShard"
	PushShard = "PushShard"
	GCShard   = "GCShard"
)

const (
	unvalid = iota
	valid
	migrating
	waitMigrate
)

type ShardComponent struct {
	ShardIndex  int
	KVDBofShard map[string]string
	ClientSeq   map[int64]int64
	State       int
}

type ShardOp struct {
	Optype    string
	Shade     ShardComponent
	Confignum int
}

func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isleader := kv.rf.GetState()
	reply.ConfigNum = kv.config.Num
	if !isleader {
		DPrintf("[%d,%d,%d]: PutShard Wrong Leader: %d", kv.gid, kv.me, kv.config.Num, args.Shade.ShardIndex)
		reply.Err = ErrWrongLeader
		return
	}
	if kv.config.Num < args.ConfigNum {
		DPrintf("[%d,%d,%d]: PutShard Wrong Config: %d", kv.gid, kv.me, kv.config.Num, args.Shade.ShardIndex)
		reply.Err = ErrWrongLeader
		return
	}
	if kv.config.Num > args.ConfigNum {
		DPrintf("[%d,%d,%d]: PutShard Out of Date: %d", kv.gid, kv.me, kv.config.Num, args.Shade.ShardIndex)
		reply.Err = ErrConfigOutOfDate
		return
	}
	if kv.config.Shards[args.Shade.ShardIndex] != kv.gid || kv.kvDB[args.Shade.ShardIndex].State != waitMigrate {
		DPrintf("[%d,%d,%d]: PutShard Wrong Shard: %d", kv.gid, kv.me, kv.config.Num, args.Shade.ShardIndex)
		reply.Err = ErrWrongGroup
		return
	}
	if kv.kvDB[args.Shade.ShardIndex].State == migrating ||
		kv.kvDB[args.Shade.ShardIndex].State == valid {
		DPrintf("[%d,%d,%d]: PutShard Already Migrate: %d", kv.gid, kv.me, kv.config.Num, args.Shade.ShardIndex)
		reply.Err = OK
		return
	}
	//state is waitMigrate
	kv.mu.Unlock()
	sop := ShardOp{
		Optype:    "PutShard",
		Shade:     args.Shade,
		Confignum: args.ConfigNum,
	}
	logindex, _, _ := kv.rf.Start(sop)

	kv.mu.Lock()
	DPrintf("[%d,%d,%d]: PutShard Start: %d,raftindex:%d", kv.gid, kv.me, kv.config.Num, args.Shade.ShardIndex, logindex)
	chForIndex, exist := kv.WaitMap[logindex]
	if !exist {
		DPrintf("[%d,%d,%d]: PutShard WaitMap: %d", kv.gid, kv.me, kv.config.Num, args.Shade.ShardIndex)
		kv.WaitMap[logindex] = make(chan WaitMsg, 1)
		chForIndex = kv.WaitMap[logindex]
	}
	defer func() {
		kv.mu.Lock()
		delete(kv.WaitMap, logindex)
		kv.mu.Unlock()
	}()

	select {
	case msg := <-chForIndex:
		DPrintf("[%d,%d,%d]: PutShard Done: %s", kv.gid, kv.me, kv.config.Num, msg.Err)
		reply.Err = msg.Err
		return
	case <-time.After(time.Duration(3000) * time.Millisecond):
		DPrintf("[%d,%d,%d]: PutShard Timeout: %d", kv.gid, kv.me, kv.config.Num, args.Shade.ShardIndex)
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) pushShard(shade ShardComponent) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, isleader := kv.rf.GetState(); !isleader {
		return
	}
	DPrintf("[%d,%d,%d]: pushShard: %d", kv.gid, kv.me, kv.config.Num, shade.ShardIndex)

	args := PushShardArgs{
		Shade:     shade,
		ConfigNum: kv.config.Num,
	}
	group := kv.config.Shards[shade.ShardIndex]
	if servers, ok := kv.config.Groups[group]; ok {
		for i := 0; i < len(servers); i++ {
			srv := kv.make_end(servers[i])
			reply := PushShardReply{}
			kv.mu.Unlock()
			ok := srv.Call("ShardKV.PushShard", &args, &reply)
			kv.mu.Lock()
			if ok && reply.Err == OK {
				DPrintf("[%d,%d,%d]: pushShard done: %d", kv.gid, kv.me, kv.config.Num, shade.ShardIndex)
				sop := ShardOp{
					Optype:    GCShard,
					Shade:     shade,
					Confignum: kv.config.Num,
				}
				kv.mu.Unlock()
				kv.rf.Start(sop)
				kv.mu.Lock()
				return
			}
			if ok && reply.Err == ErrConfigOutOfDate {
				DPrintf("[%d,%d,%d]: pushShard out of date: %d", kv.gid, kv.me, kv.config.Num, shade.ShardIndex)
				return
			}
			if ok && reply.Err == ErrWrongGroup {
				DPrintf("[%d,%d,%d]: pushShard wrong group: %d", kv.gid, kv.me, kv.config.Num, shade.ShardIndex)
				return
			}
			// wrong leader or timeout
			DPrintf("[%d,%d,%d]: pushShard to %d:%d failed: %d,%s", kv.gid, kv.me, kv.config.Num, group, i, shade.ShardIndex, reply.Err)
		}
	}
	DPrintf("[%d,%d,%d]: pushShard to %d failed: %d", kv.gid, kv.me, kv.config.Num, group, shade.ShardIndex)
}

func (kv *ShardKV) ApplyShardOp(op ShardOp, raftindex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch op.Optype {
	case "PutShard":
		DPrintf("[%d,%d,%d]: ApplyShardOp: %d,raftIndex:%d", kv.gid, kv.me, kv.config.Num, op.Shade.ShardIndex, raftindex)
		if kv.kvDB[op.Shade.ShardIndex].State != waitMigrate {
			DPrintf("[%d,%d,%d]: ApplyShardOp shade not wait migrate: %d", kv.gid, kv.me, kv.config.Num, op.Shade.ShardIndex)
			ch, channelexist := kv.WaitMap[raftindex]
			if channelexist {
				ch <- WaitMsg{Err: OK}
			}
			return
		}
		kv.kvDB[op.Shade.ShardIndex] = op.Shade
		kv.kvDB[op.Shade.ShardIndex].State = valid
		DPrintf("[%d,%d,%d]: ApplyShardOp done: %d,raftIndex:%d,shade:%d", kv.gid, kv.me, kv.config.Num, op.Shade.ShardIndex, raftindex, op.Shade.ShardIndex)
		ch, channelexist := kv.WaitMap[raftindex]
		if channelexist {
			ch <- WaitMsg{Err: OK}
		}
	case "GCShard":
		kv.gcShard(op)
	}
}

func (kv *ShardKV) gcShard(op ShardOp) {
	if kv.kvDB[op.Shade.ShardIndex].State != migrating {
		DPrintf("[%d,%d,%d]: shardGCOp shade not wait migrate: %d", kv.gid, kv.me, kv.config.Num, op.Shade.ShardIndex)
		return
	}
	kv.kvDB[op.Shade.ShardIndex] = ShardComponent{
		ShardIndex:  op.Shade.ShardIndex,
		KVDBofShard: nil,
		ClientSeq:   nil,
		State:       unvalid,
	}
	DPrintf("[%d,%d,%d]: shardGCOp done: %d", kv.gid, kv.me, kv.config.Num, op.Shade.ShardIndex)
}

func (kv *ShardKV) checkShadeMigrate(oldcfg shardctrler.Config) {
	for shard := range kv.config.Shards {
		if kv.config.Shards[shard] == oldcfg.Shards[shard] {
			continue
		}
		if kv.config.Shards[shard] == kv.gid {
			if oldcfg.Shards[shard] == 0 && kv.config.Num == 1 {
				kv.kvDB[shard].State = valid
				continue
			}
			DPrintf("[%d,%d,%d]: checkShade Wait Migrate: %d,%d->%d", kv.gid, kv.me, kv.config.Num, shard, oldcfg.Shards[shard], kv.config.Shards[shard])
			kv.kvDB[shard].State = waitMigrate
		}
		if oldcfg.Shards[shard] == kv.gid {
			DPrintf("[%d,%d,%d]: checkShade Need Migrate: %d,%d->%d", kv.gid, kv.me, kv.config.Num, shard, oldcfg.Shards[shard], kv.config.Shards[shard])
			kv.kvDB[shard].State = migrating
		}
	}
}

func (kv *ShardKV) checkShadeNeedPush() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for index, compoment := range kv.kvDB {
		if compoment.State != migrating {
			continue
		}
		DPrintf("[%d,%d,%d]: checkShadeNeedPush: %d->%d", kv.gid, kv.me, kv.config.Num, index, kv.config.Shards[index])
		go kv.pushShard(compoment)
	}
}
