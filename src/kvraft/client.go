package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

const (
	RetryTime = 100 * time.Millisecond
)

type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientId int64
	seq      int64
	leader   int

	// You will have to modify this struct.
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
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.seq = 0
	ck.leader = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	seq := atomic.AddInt64(&ck.seq, 1)
	DPrintf("Client Get: %s seq:%d", key, seq)
	server := ck.leader
	args := GetArgs{Key: key, ClientID: ck.clientId, Seq: seq}
	for {
		reply := GetReply{}
		DPrintf("Client Get: %s %d", key, seq)
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			DPrintf("Client Get Done: '%s', server: %v", reply.Value, server)
			ck.leader = server
			return reply.Value
		}
		if reply.Err == ErrNoKey {
			DPrintf("Client Get: %s, server: %v", reply.Err, server)
			return ""
		}
		server = int(nrand()) % len(ck.servers)
		time.Sleep(RetryTime)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	seq := atomic.AddInt64(&ck.seq, 1)
	server := ck.leader
	DPrintf("Client PutAppend: %s:%s->'%s' seq:%v", op, key, value, seq)
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientID: ck.clientId, Seq: seq}
	reply := PutAppendReply{}
	for {
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			DPrintf("Client PutAppend Done: %s->'%s',%s, server: %v", key, value, reply.Err, server)
			ck.leader = server
			return
		} else if !ok || reply.Err == ErrWrongLeader {
			DPrintf("Client PutAppend: ErrWrongLeader %s->'%s',%s, server: %v", key, value, reply.Err, server)
			server = int(nrand()) % len(ck.servers)
		}
		time.Sleep(RetryTime)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
