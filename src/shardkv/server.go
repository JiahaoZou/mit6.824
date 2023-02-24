package shardkv


import "6.824/labrpc"
import "6.824/raft"
import "sync"
import "6.824/labgob"
import "6.824/shardctrler"
import "time"
import "log"
import "bytes"


const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Command   string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
	ShardId   int
	Shard     Shard
	Config    shardctrler.Config
	FinishSet         map[int64]int64
}
const RPCTimeOutTime time.Duration = 100 
const UpConfigTime   time.Duration = 100

//shard将会用于在集群间传递，加上Num字段表示其在config的哪一个版本
//便于接收的集群判断其是否为最新的Shard
type Shard struct{
	Num      int
	KvMap    map[string]string 
}
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd   //string到对应server的映射
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	commitIndexCh     map[int]chan Op // 一个 index 对应一个 channel，根据提交的index决定等待哪个channel的消息
	finishSet         map[int64]int64 // 记录一个 client 已经处理过的最大 requestId，去重，防止一个client的某个请求被重复处理
	//timeoutTime       time.Duration
	shards            []Shard
	mck               *shardctrler.Clerk

	lastIncludedIndex int // 已经snapshot的最大index，Lab3B使用
	lastApplied       int

	config            shardctrler.Config  //当前config
	lastConfig        shardctrler.Config  //lastConfig
	/*config不一定是最新的config，在请求config时不是使用query(-1),而是使用query(kv.config.Num+1),config之间不能跳跃更新
	在ctrListener里面判断上一个config和当前config之间的shard迁移是否全部完成，全部完成就定时询问是否有新config*/
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	
	kv.mu.Lock()
	//检查shard是否在这一组
	if kv.config.Shards[args.ShardId] != kv.gid{
       reply.Err = ErrWrongGroup
	   kv.mu.Unlock()
	   return
	}else if kv.shards[args.ShardId].KvMap == nil{
	   reply.Err = ErrShardNotArrived
	   kv.mu.Unlock()
	   return
	}

	op := Op{
		Command   :"Get",
		Key       :args.Key,
		Value     :"",
		ClientId  :args.ClientId,
		RequestId :args.RequestId,
		ShardId    :args.ShardId,
	}
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
			kv.mu.Lock()
			if retOp.ClientId != args.ClientId||retOp.RequestId != args.RequestId {
				//判断是否达成raft共识
				reply.Err = ErrWrongLeader
			} else {
				//再次判断此时shard是否在此集群中
				if kv.config.Shards[args.ShardId] != kv.gid{
					reply.Err = ErrWrongGroup
				}else if kv.shards[args.ShardId].KvMap == nil{
					reply.Err = ErrShardNotArrived
				}else{
					reply.Err = OK
					reply.Value,have = kv.shards[args.ShardId].KvMap[args.Key]
					if !have{
						reply.Err = ErrNoKey
					}
				}
			}
			kv.mu.Unlock()
		// 没等到，超时了
		case <-time.After(RPCTimeOutTime * time.Millisecond):
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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	//检查shard是否在这一组
	if kv.config.Shards[args.ShardId] != kv.gid{
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	 }else if kv.shards[args.ShardId].KvMap == nil{
		reply.Err = ErrShardNotArrived
		kv.mu.Unlock()
		return
	 }
	op := Op{
		Command   :args.Op,
		Key       :args.Key,
		Value     :args.Value,
		ClientId  :args.ClientId,
		RequestId :args.RequestId,
		ShardId   :args.ShardId,
	}

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
			kv.mu.Lock()
			if retOp.ClientId != args.ClientId || retOp.RequestId != args.RequestId {
				reply.Err = ErrWrongLeader
			}else {
				if kv.config.Shards[args.ShardId] != kv.gid{
					reply.Err = ErrWrongGroup
				}else if kv.shards[args.ShardId].KvMap == nil{
					reply.Err = ErrShardNotArrived
				}else{
					reply.Err = OK
				}
			}
			kv.mu.Unlock()
			//putAppend
		// 没等到，超时了
		case <-time.After(RPCTimeOutTime * time.Millisecond):
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

//shard采用主动发，被动收的方法，
func (kv *ShardKV) SendShard(args *SendShardArgs, reply *SendShardReply){
	kv.mu.Lock()
    op := Op{
		Command   :"AddShard",
		ClientId  :args.ClientId,
		RequestId :args.RequestId,
		ShardId   :args.ShardId,
		Shard     :args.Shard,
		FinishSet :args.FinishSet,
	}
	index,_,isleader := kv.rf.Start(op)
    //DPrintf("index : %d , isleader %v\n",index,isleader)
    if !isleader{
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
			kv.mu.Lock()
			if retOp.ClientId != args.ClientId || retOp.RequestId != args.RequestId {
				reply.Err = ErrWrongLeader
			}else {
				if kv.config.Num < int(op.RequestId){
                     reply.Err = ErrConfigNotArrived
				}else{
					reply.Err = OK
				}
				//config的更新放到applListener里更新，此时如果应用成功应该是kv.shards[args.ShardId].Num >= args.RequestId
				//即我们现在的状态已经比传过来的shard的状态更新或者相同，就返回OK
			}
			kv.mu.Unlock()
		//putAppend
		// 没等到，超时了
		case <-time.After(RPCTimeOutTime * time.Millisecond):
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


//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


func (kv *ShardKV) applyListener(){
	for{
        DPrintf("AAAAAAAAAAleader: %d, group: %d\n",kv.me,kv.gid)
		applyMsg := <- kv.applyCh
		DPrintf("BBBBBBBBBBleader: %d, group: %d\n",kv.me,kv.gid)
        if applyMsg.CommandValid {
			
            
            op := applyMsg.Command.(Op) //interface 类型转换
			index := int64(applyMsg.CommandIndex)
			requestId:=op.RequestId  
			//去重
			kv.mu.Lock()
			if applyMsg.CommandIndex <= kv.lastApplied{
				kv.mu.Unlock()
				continue
			}
			if kv.lastApplied < applyMsg.CommandIndex{
				kv.lastApplied = applyMsg.CommandIndex
			}
			
			//client传过来的对数据操作的RPC
			if op.Command=="Put"||op.Command=="Append"||op.Command=="Get"{
                
				if kv.config.Shards[op.ShardId] != kv.gid || kv.shards[op.ShardId].KvMap == nil{
					//
				}else{
					DPrintf("APPLY:1111111,%v\n",op.Command)
					finishIndex,have := kv.finishSet[op.ClientId]
			        isDuplicate:=false
			        if have && requestId <= finishIndex{
                        isDuplicate = true
			        }
			        if !isDuplicate{
				    //如果不重复，要将结果应用于db
                        switch op.Command{
					    case "Put":
							kv.shards[op.ShardId].KvMap[op.Key] = op.Value
					    case "Append":
							kv.shards[op.ShardId].KvMap[op.Key] += op.Value
					    case "Get":
                            //DPrintf("Get\n")
					    }
			        }
				}
				
			}else{
				//更新config版本或者安装Shard,不需要检查shard
			    DPrintf("apply:2222222,%v,%d\n",op.Command,applyMsg.CommandIndex)
				switch op.Command{
				case "AddShard":
                    kv.addShardHandler(op)
				case "UpConfig":
                    kv.upConfigHandler(op)
				case "RemoveShard":
                    kv.removeShardHandler(op)
				}
			}
			kv.mu.Unlock()
			//判断是否要进行snapshot
			//如果进行，则将db和finishset保存，此时db中的最后一条command就是当前接收到的一条，
			//所以snapshot中的lastIndex就是applyMsg中的applyMsg.CommandIndex
			kv.mu.Lock()
			if kv.maxraftstate !=-1 &&kv.rf.GetRaftStateSize()>kv.maxraftstate{
				snapshot := kv.PersistSnapShot()
				kv.rf.Snapshot(applyMsg.CommandIndex,snapshot)
			}
			//DPrintf("applyListen: key:%v, value %v\n",op.Key,op.Value)
			ch, exist := kv.commitIndexCh[int(index)]
	        if exist {
		        ch <- op
	        }
			kv.mu.Unlock()
		}else if applyMsg.SnapshotValid {
            //
			DPrintf("APPLY: 666666leader: %d, group: %d\n",kv.me,kv.gid)
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

/*
type Op struct {
	Command   string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
	ShardId   int
	Shard     Shard
	Config    shardctrler.Config
}
*/
//外层有锁，不需要锁
func (kv *ShardKV) upConfigHandler(op Op){
    curConfig := kv.config
	newConfig := op.Config
	if curConfig.Num >= newConfig.Num{
		return
	}
	//如果新config和旧config都拥有某个shard，则应该将这些shard的num更新为新的num
	//注意，在新的中而不在旧config中的shard的num不应该被更新
	for shardId,gid := range newConfig.Shards{
		/*
		if gid == kv.gid {
			if curConfig.Shards[shardId] == gid{
				kv.shards[shardId].Num = newConfig.Num
			}
			
		}
		*/
		if gid == kv.gid && curConfig.Shards[shardId] == 0{
			kv.shards[shardId].KvMap = make(map[string]string)
			kv.shards[shardId].Num  = newConfig.Num
		}
	}
	kv.lastConfig = curConfig
	kv.config = newConfig

}


func (kv *ShardKV) addShardHandler(op Op){
    if kv.shards[op.ShardId].KvMap != nil || op.Shard.Num < kv.config.Num{
	    return	
	}
	
	newMap := make(map[string]string)
	for key,value := range op.Shard.KvMap{
		newMap[key] = value
	}
	kv.shards[op.ShardId].KvMap = newMap
	kv.shards[op.ShardId].Num = kv.config.Num

 
   
	for clientId,requestId := range op.FinishSet{
		if r,ok := kv.finishSet[clientId];!ok || r<requestId{
			kv.finishSet[clientId] = requestId
		}
	}
}


func (kv *ShardKV) removeShardHandler(op Op){
	//如果kv.config.Num 大于 op.Num，侧说明，此时集群已经进入了新的config，上一个config的·remove应该是已经完成了的
    if op.RequestId < int64(kv.config.Num){
		return 
	}
	kv.shards[op.ShardId].KvMap = nil
	kv.shards[op.ShardId].Num = int(op.RequestId)
}

func (kv *ShardKV) isAllSend() bool{
	for shard,gid := range kv.lastConfig.Shards{
		//哪些shard应该传？
		//上个config中该shard属于此组，当前config中不属于此组，
		//如何判断一个shard是否传完，shard中有Num字段，如果接到对方接收shard的回应，则将Num改为当前Num
		//
		if gid == kv.gid && kv.config.Shards[shard]!=kv.gid && kv.shards[shard].Num<kv.config.Num{
			return false
		}
	}
	return true
}
func (kv *ShardKV) isAllReceived() bool{
	for shard,gid:= range kv.lastConfig.Shards{
		//该关注哪些shard是否已经收到？与上面正好相反
		if gid != kv.gid && kv.config.Shards[shard]==kv.gid && kv.shards[shard].Num<kv.config.Num{
			return false
		}
	}
	return true
}
//定时询问shardctrler版本信息，并生成新版本日志，放到raft层进行共识
//并在版本变化后向目标集群传递不再属于自己的Shard
//只有leader需要执行
func (kv *ShardKV) kvctrListener(){
    kv.mu.Lock()
	//curConfig := kv.config
	rf := kv.rf
	kv.mu.Unlock()

	for{
        if _,isleader := rf.GetState();!isleader{
			time.Sleep(UpConfigTime*time.Millisecond)
			continue
		}
		
		//leader
		kv.mu.Lock()
		DPrintf("ccccccccccccc, leader: %d, group: %d\n",kv.me,kv.gid)
		//没有送
        if !kv.isAllSend(){
			for shardId,gid := range kv.lastConfig.Shards{
				
				if gid == kv.gid && kv.config.Shards[shardId]!=kv.gid && kv.shards[shardId].Num<kv.config.Num{
					sendShard := kv.shards[shardId]
					sendShard.Num = kv.config.Num
                    
					serverList := kv.config.Groups[kv.config.Shards[shardId]]
					servers := make([]*labrpc.ClientEnd,len(serverList))
					for i,servername := range serverList{
						servers[i] = kv.make_end(servername)
					}
					args := SendShardArgs{
						ShardId : shardId,
						Shard   : sendShard,
						ClientId : int64(gid),
						RequestId: int64(kv.config.Num),
						FinishSet: kv.finishSet,
					}

					go func([]*labrpc.ClientEnd,SendShardArgs) {
						start := time.Now()
						i:=0
						for {
							reply := SendShardReply{}
                            
                            ok := servers[i].Call("ShardKV.SendShard", &args, &reply)
							if ok&&reply.Err == "OK"|| time.Now().Sub(start)>=5*time.Second{ //并不关注别的err
								kv.mu.Lock()
								op := Op{
									Command   : "RemoveShard",
									ClientId  : int64(kv.gid),
									RequestId : int64(kv.config.Num),
									ShardId   : args.ShardId,
								}
								
								//等对方回应了RPC，此时自己可能已经不是leader了，那么这个op可能存在发不到raft层的情况，
								//此时，在自己这个group看来，这个shard是没有发成功的，而对方group看来，是发成功了的
								//自己group的新leader会再次发送此shard，这就要求对方的group在收到已经安装的shard的安装请求时，要立刻回复一个ok
								kv.rf.Start(op)
								kv.mu.Unlock()
								break;
							}
							i = (i+1)%len(servers)
							if i==0{
								time.Sleep(UpConfigTime*time.Millisecond)
							}
						}
					}(servers,args)
				}
			}
			kv.mu.Unlock()
			time.Sleep(UpConfigTime*time.Millisecond)
			continue
		}
        //没有将此轮的shard接收完成，是不允许进入新一轮config的(如果存在的话)
		if !kv.isAllReceived(){
			kv.mu.Unlock()
			time.Sleep(UpConfigTime*time.Millisecond)
			continue
		}
        DPrintf("1111111\n")
		//此轮shard已经全部发送和接收完成，可以查看是否有新的config，注意，查看kv.comfig.Num+1,不允许跳跃更新
		mck := kv.mck
		curNum := kv.config.Num
		kv.mu.Unlock()
		
		newConfig := mck.Query(curNum + 1)

		if newConfig.Num != curNum + 1{
			time.Sleep(UpConfigTime*time.Millisecond)
			continue
		}
		//有newConfig
		DPrintf("22222222222222\n")
		op:=Op{
			Command   : "UpConfig",
			Config    : newConfig,
		}
		rf.Start(op)
	}
}
//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
    kv.shards = make([]Shard, shardctrler.NShards)
	kv.commitIndexCh = make(map[int]chan Op)  
	kv.finishSet     = make(map[int64]int64) 
	
	// Use something like this to talk to the shardctrler:
	//这个server对shardctrler而言相当于客户端client
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.applyListener()
	go kv.kvctrListener()

	return kv
}

func (kv *ShardKV) PersistSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.shards)
	err = e.Encode(kv.finishSet)
	err = e.Encode(kv.maxraftstate)
	err = e.Encode(kv.config)
	err = e.Encode(kv.lastConfig)
	if err != nil {
		//log.Fatalf("[%d-%d] fails to take snapshot.", kv.gid, kv.me)
	}
	return w.Bytes()
}

// DecodeSnapShot install a given snapshot
func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var shards []Shard
	var finishSet map[int64]int64
	var maxraftstate int
	var config, lastConfig shardctrler.Config

	if d.Decode(&shards) != nil || d.Decode(&finishSet) != nil ||
		d.Decode(&maxraftstate) != nil || d.Decode(&config) != nil || d.Decode(&lastConfig) != nil {
		//log.Fatalf("[Server(%v)] Failed to decode snapshot！！！", kv.me)
	} else {
		kv.shards = shards
		kv.finishSet = finishSet
		kv.maxraftstate = maxraftstate
		kv.config = config
		kv.lastConfig = lastConfig

	}
}

