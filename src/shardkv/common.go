package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrWrongLeader  = "ErrWrongLeader"
	ErrShardNoValid = "ErrShardNoValid"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ConfigNum int
	ClientID  int64
	Seq       int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ConfigNum int
	ClientID  int64
	Seq       int64
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardArg struct {
	Shard     int
	ConfigNum int
}

type ShardReply struct {
	Err      Err
	ShardMap map[string]string
	SeqMap   map[int64]int64
}
