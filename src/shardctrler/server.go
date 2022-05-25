package shardctrler

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	RaftlastIncludedIndex int
	// Your data here.
	WaitMap map[int]chan WaitMsg //key:index in raft, value:channel
	seqMap  map[int64]int64

	configs []Config // indexed by config num
}

type WaitMsg struct {
	Err Err
	Op  Op
}

type Op struct {
	// Your data here.
	OpType    string
	JoinArgs  *JoinArgs
	LeaveArgs *LeaveArgs
	MoveArgs  *MoveArgs
	QueryArgs *QueryArgs
	ClientID  int64
	Seq       int64
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	DPrintf("%d: Join:%v", sc.me, args)
	r := sc.SendOperation(Op{
		OpType:   "Join",
		JoinArgs: args,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	})
	reply.WrongLeader = r.WrongLeader
	reply.Err = r.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	DPrintf("%d: Leave:%v", sc.me, args)
	r := sc.SendOperation(Op{
		OpType:    "Leave",
		LeaveArgs: args,
		ClientID:  args.ClientID,
		Seq:       args.Seq,
	})
	reply.WrongLeader = r.WrongLeader
	reply.Err = r.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	DPrintf("%d: Move:%v", sc.me, args)
	r := sc.SendOperation(Op{
		OpType:   "Move",
		MoveArgs: args,
		ClientID: args.ClientID,
		Seq:      args.Seq,
	})
	reply.WrongLeader = r.WrongLeader
	reply.Err = r.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	DPrintf("%d: Query:%v", sc.me, args)
	r := sc.SendOperation(Op{
		OpType:    "Query",
		QueryArgs: args,
		ClientID:  args.ClientID,
		Seq:       args.Seq,
	})
	if r.Err != OK {
		reply.Err = r.Err
		reply.WrongLeader = r.WrongLeader
		return
	}
	sc.mu.Lock()
	cfg := sc.configs[len(sc.configs)-1]
	if args.Num != -1 && args.Num < len(sc.configs) {
		cfg = sc.configs[args.Num]
	}
	DPrintf("%d: Query:%v,reply:%v,\n%v", sc.me, args, reply, sc.configs)
	sc.mu.Unlock()
	reply.Config = cfg
	reply.WrongLeader = r.WrongLeader
	reply.Err = r.Err
}

func (sc *ShardCtrler) SendOperation(op Op) JoinReply {
	// Your code here.
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		return JoinReply{
			WrongLeader: true,
			Err:         ErrWrongLeader,
		}
	}
	index, _, _ := sc.rf.Start(op)
	DPrintf("%d: SendOperation:%s,index:%d", sc.me, op.OpType, index)
	sc.mu.Lock()
	chForRaftIndex, exist := sc.WaitMap[index]
	if !exist {
		sc.WaitMap[index] = make(chan WaitMsg, 1)
		chForRaftIndex = sc.WaitMap[index]
	}
	sc.mu.Unlock()
	defer func() {
		sc.mu.Lock()
		delete(sc.WaitMap, index)
		sc.mu.Unlock()
	}()
	select {
	case waitmsg := <-chForRaftIndex:
		{
			return JoinReply{
				WrongLeader: false,
				Err:         waitmsg.Err,
			}
		}
	case <-time.After(time.Second * 5):
		DPrintf("%d: DoOperation:%v,index:%v,timeout", sc.me, op, index)
		return JoinReply{
			WrongLeader: true,
			Err:         ErrWrongLeader,
		}
	}
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		applyMsg := <-sc.applyCh
		sc.mu.Lock()
		if !applyMsg.CommandValid {
			sc.mu.Unlock()
			continue
		}
		if applyMsg.CommandIndex <= sc.RaftlastIncludedIndex {
			sc.mu.Unlock()
			continue
		}
		op := applyMsg.Command.(Op)
		if seq, exist := sc.seqMap[op.ClientID]; !(exist && seq >= op.Seq) {
			switch op.OpType {
			case "Join":
				{
					DPrintf("%d: applier Join:%v", sc.me, op.JoinArgs.Servers)
					oldcfg := sc.configs[len(sc.configs)-1]
					newcfg := newconfig(oldcfg)
					for k, v := range op.JoinArgs.Servers {
						newcfg.Groups[k] = v
					}
					newcfg.rebalance()
					DPrintf("%d: applier Join:%v,oldcfg:%v,newcfg:%v", sc.me, op.JoinArgs.Servers, oldcfg, newcfg)
					sc.configs = append(sc.configs, newcfg)
				}
			case "Leave":
				{
					DPrintf("%d: applier Leave:%v", sc.me, op.LeaveArgs.GIDs)
					oldcfg := sc.configs[len(sc.configs)-1]
					newcfg := newconfig(oldcfg)
					for _, gid := range op.LeaveArgs.GIDs {
						delete(newcfg.Groups, gid)
					}
					newcfg.rebalance()
					DPrintf("%d: applier Leave:%v,oldcfg:%v,newcfg:%v", sc.me, op.LeaveArgs.GIDs, oldcfg, newcfg)
					sc.configs = append(sc.configs, newcfg)
				}
			case "Move":
				{
					DPrintf("%d: applier Move:%v", sc.me, op.MoveArgs.Shard)
					oldcfg := sc.configs[len(sc.configs)-1]
					newcfg := newconfig(oldcfg)
					newcfg.move(op.MoveArgs.Shard, op.MoveArgs.GID)
					DPrintf("%d: applier Move:%v,oldcfg:%v,newcfg:%v", sc.me, op.MoveArgs.Shard, oldcfg, newcfg)
					sc.configs = append(sc.configs, newcfg)
				}
			case "Query":
			default:
			}
			sc.seqMap[op.ClientID] = op.Seq
		}
		ch, exist := sc.WaitMap[applyMsg.CommandIndex]
		sc.mu.Unlock()
		if exist {
			ch <- WaitMsg{
				Err: OK,
				Op:  op,
			}
		}
	}
}

func (cfg *Config) move(Shard int, GID int) {
	cfg.Shards[Shard] = GID
}

func (cfg *Config) rebalance() {
	if len(cfg.Groups) == 0 {
		cfg.Shards = [10]int{}
		return
	}
	groupshardsmap := make(map[int][]int)
	unassignedshards := make([]int, 0)
	for group := range cfg.Groups {
		groupshardsmap[group] = make([]int, 0)
	}
	for shard, group := range cfg.Shards {
		if _, exist := groupshardsmap[group]; !exist {
			cfg.Shards[shard] = 0
			unassignedshards = append(unassignedshards, shard)
			continue
		}
		groupshardsmap[group] = append(groupshardsmap[group], shard)
	}
	groupshardslice := make([]int, 0)
	for group := range groupshardsmap {
		groupshardslice = append(groupshardslice, group)
	}
	n := len(groupshardslice)
	for {
		sort.Slice(groupshardslice, func(i, j int) bool {
			return len(groupshardsmap[groupshardslice[i]]) < len(groupshardsmap[groupshardslice[j]]) || // sort by length
				(len(groupshardsmap[groupshardslice[i]]) == len(groupshardsmap[groupshardslice[j]]) &&
					groupshardslice[i] < groupshardslice[j]) //if length is same, sort by id
		})
		if len(unassignedshards) != 0 {
			groupshardsmap[groupshardslice[0]] = append(groupshardsmap[groupshardslice[0]], unassignedshards...)
			unassignedshards = make([]int, 0)
			continue
		}
		DPrintf("rebalance:%v", groupshardslice)
		if math.Abs(float64(len(groupshardsmap[groupshardslice[n-1]])-len(groupshardsmap[groupshardslice[0]]))) <= 1 {
			break
		}
		DPrintf("rebalance:%v", groupshardsmap)
		unassignedshards = append(unassignedshards, groupshardsmap[groupshardslice[n-1]][0])
		groupshardsmap[groupshardslice[n-1]] = groupshardsmap[groupshardslice[n-1]][1:]
	}
	DPrintf("rebalance done:%v", groupshardsmap)
	for group, shards := range groupshardsmap {
		for _, shard := range shards {
			cfg.Shards[shard] = group
		}
	}
}

func newconfig(config Config) Config {
	newcfg := config
	newcfg.Num++
	newcfg.Shards = config.Shards
	newcfg.Groups = make(map[int][]string)
	for k, v := range config.Groups {
		newcfg.Groups[k] = v
	}
	return newcfg
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	DPrintf("%d: Killed...", sc.me)
	sc.rf.Kill()
	atomic.StoreInt32(&sc.dead, 1)
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.seqMap = make(map[int64]int64)
	sc.WaitMap = make(map[int]chan WaitMsg)
	go sc.applier()
	DPrintf("%d: Started...", sc.me)

	return sc
}
