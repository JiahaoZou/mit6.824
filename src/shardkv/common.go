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
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrShardNotArrived = "ShardNotArrived"
	ErrConfigNotArrived = "ConfigNotArrived"
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
	ClientId  int64
	RequestId int64
	ShardId   int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId  int64
	RequestId int64
	ShardId   int
}

type GetReply struct {
	Err   Err
	Value string
}

type SendShardArgs struct{
    Shard     Shard
	ShardId   int
	ClientId  int64
	RequestId int64
	FinishSet map[int64]int64
}

type SendShardReply struct{
	Err   Err
}
