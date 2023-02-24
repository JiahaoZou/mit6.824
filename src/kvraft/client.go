package kvraft

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
//import "sync/atomic"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId    int64
	requestId   int64
	leaderId    int
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
    ck.requestId = 0
	ck.leaderId = 0
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
	
    ck.requestId++
	args := GetArgs{
		Key   :key,
		ClientId  :ck.clientId,
		RequestId :ck.requestId,
	}
	leaderId := ck.leaderId
	for{
	    reply := GetReply{}
		
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if ok {
			switch reply.Err {
			case ErrWrongLeader: //找新leader
				leaderId = (leaderId+1)%len(ck.servers)
				continue
			case ErrNoKey:
				ck.leaderId = leaderId
				return ""
			case OK://再次发送RPC
			    ck.leaderId = leaderId
			    return reply.Value
			}
		}
		leaderId = (leaderId+1)%len(ck.servers)
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
	ck.requestId++
	args := PutAppendArgs{
		Key   :key,
	    Value :value,
	    Op    :op, 
		ClientId  :ck.clientId,
		RequestId :ck.requestId,
	}
	leaderId := ck.leaderId
	for{
	    reply := PutAppendReply{}
		
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			switch reply.Err {
			case ErrWrongLeader: //找新leader
				leaderId = (leaderId+1)%len(ck.servers)
				continue
			case OK:
			    ck.leaderId = leaderId
			    return 
			}
		}
		leaderId = (leaderId+1)%len(ck.servers)
	}
	
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
