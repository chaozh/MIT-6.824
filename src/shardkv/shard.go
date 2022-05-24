package shardkv

import (
	"time"

	"6.824/shardctrler"
)

type ShardOp struct {
	Optype    string
	Shade     ShardComponent
	Confignum int
}

func (kv *ShardKV) PushShard(args *PushShardArgs, reply *PushShardReply) {
	kv.mu.Lock()
	_, isleader := kv.rf.GetState()
	reply.ConfigNum = kv.config.Num
	if !isleader {
		DPrintf("[%d,%d,%d]: PutShard Wrong Leader: %d", kv.gid, kv.me, kv.config.Num, args.Shade.ShardIndex)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if kv.config.Num < args.ConfigNum {
		DPrintf("[%d,%d,%d]: PutShard Wrong Config: %d", kv.gid, kv.me, kv.config.Num, args.Shade.ShardIndex)
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if kv.config.Num > args.ConfigNum {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
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
	kv.mu.Unlock()
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
	for {
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
					if reply.ConfigNum > kv.config.Num {
						return
					}
					kv.kvNeedSend[shade.ShardIndex] = false
					return
				}
				DPrintf("[%d,%d,%d]: pushShard to %d:%d failed: %d,%s", kv.gid, kv.me, kv.config.Num, group, i, shade.ShardIndex, reply.Err)
			}
		}
		DPrintf("[%d,%d,%d]: pushShard to %d something: %d", kv.gid, kv.me, kv.config.Num, group, shade.ShardIndex)
	}
}

func (kv *ShardKV) ApplyShardOp(op ShardOp, raftindex int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch op.Optype {
	case "PutShard":
		DPrintf("[%d,%d,%d]: ApplyShardOp: %d,raftIndex:%d", kv.gid, kv.me, kv.config.Num, op.Shade.ShardIndex, raftindex)
		if !kv.kvDB[op.Shade.ShardIndex].IsMigrateing {
			ch, channelexist := kv.WaitMap[raftindex]
			if channelexist {
				ch <- WaitMsg{Err: OK}
			}
			return
		}
		kv.kvDB[op.Shade.ShardIndex] = op.Shade
		kv.kvDB[op.Shade.ShardIndex].IsMigrateing = false
		kv.kvNeedSend[op.Shade.ShardIndex] = false
		DPrintf("[%d,%d,%d]: ApplyShardOp done: %d,raftIndex:%d,shade:%d", kv.gid, kv.me, kv.config.Num, op.Shade.ShardIndex, raftindex, op.Shade.ShardIndex)
		ch, channelexist := kv.WaitMap[raftindex]
		if channelexist {
			ch <- WaitMsg{Err: OK}
		}
	case "PushShard":
		go kv.pushShard(op.Shade)
	}
}

func (kv *ShardKV) checkShadeNeedMigrate(oldcfg shardctrler.Config) {
	for shard := range kv.config.Shards {
		if kv.config.Shards[shard] == oldcfg.Shards[shard] {
			continue
		}
		if kv.config.Shards[shard] == kv.gid {
			if oldcfg.Shards[shard] == 0 && kv.config.Num == 1 {
				continue
			}
			DPrintf("[%d,%d,%d]: checkShadeNeedMigrate: %d,%d,%d", kv.gid, kv.me, kv.config.Num, shard, kv.config.Shards[shard], oldcfg.Shards[shard])
			kv.kvDB[shard].IsMigrateing = true
		}
		if oldcfg.Shards[shard] == kv.gid {
			kv.kvNeedSend[shard] = true
		}
	}
}

func (kv *ShardKV) checkShadeNeedPush() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for shard, issend := range kv.kvNeedSend {
		if !issend {
			continue
		}
		DPrintf("[%d,%d,%d]: checkShadeNeedPush: %d->%d", kv.gid, kv.me, kv.config.Num, shard, kv.config.Shards[shard])
		sop := ShardOp{Optype: "PushShard", Shade: kv.kvDB[shard]}
		kv.mu.Unlock()
		kv.rf.Start(sop)
		kv.mu.Lock()
	}
}
