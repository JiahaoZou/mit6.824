package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
	"bytes"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Command   string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commitIndexCh     map[int]chan Op // 一个 index 对应一个 channel，根据提交的index决定等待哪个channel的消息
	finishSet         map[int64]int64 // 记录一个 client 已经处理过的最大 requestId，去重，防止一个client的某个请求被重复处理
    db                map[string]string
	lastIncludedIndex int // 已经snapshot的最大index，Lab3B使用
	lastApplied       int

 	timeoutTime       time.Duration
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed(){
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Command   :"Get",
		Key       :args.Key,
		Value     :"",
		ClientId  :args.ClientId,
		RequestId :args.RequestId,
	}
	kv.mu.Lock()
	//检查是否leader
	index,_,isleader := kv.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch, have := kv.commitIndexCh[index]
	if !have {
		ch = make(chan Op, 1)
		kv.commitIndexCh[index] = ch
	}
    kv.mu.Unlock()

	select {
		// 等待从 index 对应的 channel 中传来 Op
		case retOp := <-ch:
			if retOp.ClientId != args.ClientId||retOp.RequestId != args.RequestId {
				reply.Err = ErrWrongLeader
			} else {
				reply.Err = OK
				kv.mu.Lock()
				reply.Value,have = kv.db[args.Key]
				kv.mu.Unlock()
				if !have{
					reply.Err = ErrNoKey
				}
			}
		// 没等到，超时了
		case <-time.After(kv.timeoutTime * time.Millisecond):
			reply.Err = ErrWrongLeader
	}
	//
	kv.mu.Lock()
	ch, have = kv.commitIndexCh[index]
	if have {
		close(ch)
		delete(kv.commitIndexCh,index)
	}
	kv.mu.Unlock()
}

/*
    Key   string
	Value string
	Op    string
*/
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//
	if kv.killed(){
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		Command   :args.Op,
		Key       :args.Key,
		Value     :args.Value,
		ClientId  :args.ClientId,
		RequestId :args.RequestId,
	}
	kv.mu.Lock()
	//检查是否为leader
	index,_,isleader := kv.rf.Start(op)
    //DPrintf("index : %d , isleader %v\n",index,isleader)
    if !isleader{
       reply.Err = ErrWrongLeader
	   kv.mu.Unlock()
	   return
	}
    
	//创建一个channel用于接受applyListen的返回
	ch, have := kv.commitIndexCh[index]
	if !have {
		ch = make(chan Op, 1)
		kv.commitIndexCh[index] = ch
	}
	kv.mu.Unlock()
	select {
		// 等待从 index 对应的 channel 中传来 Op
		case retOp := <-ch:
			if retOp.ClientId != args.ClientId || retOp.RequestId != args.RequestId {
				reply.Err = ErrWrongLeader
			}else {
				reply.Err = OK
			}
		// 没等到，超时了
		case <-time.After(kv.timeoutTime * time.Millisecond):
			reply.Err = ErrWrongLeader
	}
	kv.mu.Lock()
	ch, have = kv.commitIndexCh[index]
	if have {
		close(ch)
		delete(kv.commitIndexCh,index)
	}
	
	kv.mu.Unlock()
}


func (kv *KVServer) DecodeSnapShot(snapshot []byte){
    if snapshot==nil||len(snapshot)<1{
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvPersist map[string]string
	var finishSet map[int64]int64

	if d.Decode(&kvPersist)!=nil ||
	   d.Decode(&finishSet)!=nil{

	}else{
		kv.db = kvPersist
		kv.finishSet = finishSet
	}
}
func (kv *KVServer) PersistSnapShot() []byte{
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.finishSet)
	data := w.Bytes()
	return data
}
/*
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type Op struct {
	Command   string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
}
*/
func (kv *KVServer) applyListen(){
	for{
		if kv.killed(){
			//DPrintf("AAAAAA\n")
			return
		}
		applyMsg := <- kv.applyCh
        if applyMsg.CommandValid {
			
            if applyMsg.CommandIndex <= kv.lastIncludedIndex{
				return
			}
            
            op := applyMsg.Command.(Op) //interface 类型转换
			index := int64(applyMsg.CommandIndex)
			requestId:=op.RequestId  
			//去重
			kv.mu.Lock()
			if kv.lastApplied < applyMsg.CommandIndex{
				kv.lastApplied = applyMsg.CommandIndex
			}
			finishIndex,have := kv.finishSet[op.ClientId]
			isDuplicate:=false
			if have && requestId <= finishIndex{
                isDuplicate = true
			}
			kv.mu.Unlock()
            resOp := op
			if !isDuplicate{
				//如果不重复，要将结果应用于db
				kv.mu.Lock()
                if op.Command == "Put"{
					kv.db[op.Key] = op.Value
				}else if op.Command == "Append"{
					kv.db[op.Key] += op.Value
				}
				kv.finishSet[op.ClientId] = requestId
				
				//判断是否要进行snapshot
			    //如果进行，则将db和finishset保存，此时db中的最后一条command就是当前接收到的一条，
			    //所以snapshot中的lastIndex就是applyMsg中的applyMsg.CommandIndex
				if kv.maxraftstate !=-1 &&kv.rf.GetRaftStateSize()>kv.maxraftstate{
				
					snapshot := kv.PersistSnapShot()
					kv.rf.Snapshot(applyMsg.CommandIndex,snapshot)
				}
				kv.mu.Unlock()
			}
			
			
			
			kv.mu.Lock()
			//DPrintf("applyListen: key:%v, value %v\n",op.Key,op.Value)
			ch, exist := kv.commitIndexCh[int(index)]
			
	        if exist {
		        ch <- resOp
	        }
			kv.mu.Unlock()
		}else if applyMsg.SnapshotValid {
            //
			kv.mu.Lock()
			//DPrintf("applyMsg.SnapshotIndex: %d, kv.lastIncludedIndex:%d\n",applyMsg.SnapshotIndex,kv.lastIncludedIndex)
			if applyMsg.SnapshotIndex > kv.lastIncludedIndex{
				kv.lastIncludedIndex = applyMsg.SnapshotIndex
			}
			if kv.lastIncludedIndex>kv.lastApplied{
				kv.DecodeSnapShot(applyMsg.Snapshot)
				kv.lastApplied = kv.lastIncludedIndex
			}
			kv.mu.Unlock()
		}
		
	}
}
//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
    kv.timeoutTime = 100
    kv.commitIndexCh = make(map[int]chan Op)   // 一个 index 对应一个 channel，根据提交的index决定等待哪个channel的消息
	kv.finishSet     = make(map[int64]int64)   // 记录一个 client 已经处理过的最大 requestId，去重，防止一个client的某个请求被重复处理
    kv.db            = make(map[string]string)    
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.lastIncludedIndex = -1
    kv.lastApplied = -1
	//从本地恢复snapshot
	snapshot := persister.ReadSnapshot()
	if len(snapshot)>0{
		kv.DecodeSnapShot(snapshot)
	}
	// You may need initialization code here.
      
    go kv.applyListen()

	return kv
}
