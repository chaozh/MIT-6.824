package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64
	seq      int64
	leader   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.seq = 0
	ck.leader = 0

	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	seq := atomic.AddInt64(&ck.seq, 1)
	server := ck.leader
	args := &QueryArgs{Num: num, ClientID: ck.clientId, Seq: seq}
	for {
		reply := QueryReply{}
		ok := ck.servers[server].Call("ShardCtrler.Query", args, &reply)
		if ok && !reply.WrongLeader {
			ck.leader = server
			DPrintf("Query Config: %v,seq: %d", reply.Config, seq)
			return reply.Config
		}
		server = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	seq := atomic.AddInt64(&ck.seq, 1)
	server := ck.leader
	args := &JoinArgs{ClientID: ck.clientId, Seq: seq, Servers: servers}

	for {
		var reply JoinReply
		ok := ck.servers[server].Call("ShardCtrler.Join", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		server = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Leave(gids []int) {

	// Your code here.
	seq := atomic.AddInt64(&ck.seq, 1)
	server := ck.leader
	args := &LeaveArgs{ClientID: ck.clientId, Seq: seq, GIDs: gids}

	for {
		// try each known server.
		var reply LeaveReply
		ok := ck.servers[server].Call("ShardCtrler.Leave", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		server = (server + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Move(shard int, gid int) {

	// Your code here.
	seq := atomic.AddInt64(&ck.seq, 1)
	server := ck.leader
	args := &MoveArgs{Shard: shard, GID: gid, ClientID: ck.clientId, Seq: seq}

	for {
		// try each known server.
		var reply MoveReply
		ok := ck.servers[server].Call("ShardCtrler.Move", args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		server = (server + 1) % len(ck.servers)
	}
}
