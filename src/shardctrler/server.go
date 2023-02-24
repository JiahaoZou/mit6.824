package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "time"
import "log"


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	commitIndexCh     map[int]chan Op // 一个 index 对应一个 channel，根据提交的index决定等待哪个channel的消息
	finishSet         map[int64]int64 // 记录一个 client 已经处理过的最大 requestId，去重，防止一个client的某个请求被重复处理
	timeoutTime       time.Duration

	configs []Config // indexed by config num
}
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
const (
	JoinType = iota
	LeaveType
	MoveType
	QueryType
)


type Op struct {
	// Your data here.
    OpType       int
	ClientId     int64
	RequestId    int64
	//join
	JoinServers  map[int][]string
	//leave
	LeaveGIDs    []int
	//Move
	MoveShard    int
	MoveGID       int
	//Query
	QueryNum     int
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		OpType      :JoinType,
		ClientId    :args.ClientId,
		RequestId   :args.RequestId,
		JoinServers :args.Servers,
	}
	sc.mu.Lock()
	//检查是否leader
	index,_,isleader := sc.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	ch, have := sc.commitIndexCh[index]
	if !have {
		ch = make(chan Op, 1)
		sc.commitIndexCh[index] = ch
	}
    sc.mu.Unlock()

	select {
		// 等待从 index 对应的 channel 中传来 Op
		case retOp := <-ch:
			if retOp.ClientId != args.ClientId||retOp.RequestId != args.RequestId {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
			} else {
				reply.Err = OK
				reply.WrongLeader = false
			}
		// 没等到，超时了
		case <-time.After(sc.timeoutTime * time.Millisecond):
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
	}
	//
	sc.mu.Lock()
	ch, have = sc.commitIndexCh[index]
	if have {
		close(ch)
		delete(sc.commitIndexCh,index)
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		OpType    :LeaveType,
		ClientId  :args.ClientId,
		RequestId :args.RequestId,
		LeaveGIDs :args.GIDs,
	}
	sc.mu.Lock()
	//检查是否leader
	index,_,isleader := sc.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	ch, have := sc.commitIndexCh[index]
	if !have {
		ch = make(chan Op, 1)
		sc.commitIndexCh[index] = ch
	}
    sc.mu.Unlock()

	select {
		// 等待从 index 对应的 channel 中传来 Op
		case retOp := <-ch:
			if retOp.ClientId != args.ClientId||retOp.RequestId != args.RequestId {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
			} else {
				reply.Err = OK
				reply.WrongLeader = false
			}
		// 没等到，超时了
		case <-time.After(sc.timeoutTime * time.Millisecond):
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
	}
	//
	sc.mu.Lock()
	ch, have = sc.commitIndexCh[index]
	if have {
		close(ch)
		delete(sc.commitIndexCh,index)
	}
	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		OpType    :MoveType,
		ClientId  :args.ClientId,
		RequestId :args.RequestId,
		MoveShard :args.Shard,
		MoveGID   :args.GID,
	}
	sc.mu.Lock()
	//检查是否leader
	index,_,isleader := sc.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	ch, have := sc.commitIndexCh[index]
	if !have {
		ch = make(chan Op, 1)
		sc.commitIndexCh[index] = ch
	}
    sc.mu.Unlock()

	select {
		// 等待从 index 对应的 channel 中传来 Op
		case retOp := <-ch:
			if retOp.ClientId != args.ClientId||retOp.RequestId != args.RequestId {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
			} else {
				reply.Err = OK
				reply.WrongLeader = false
			}
		// 没等到，超时了
		case <-time.After(sc.timeoutTime * time.Millisecond):
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
	}
	//
	sc.mu.Lock()
	ch, have = sc.commitIndexCh[index]
	if have {
		close(ch)
		delete(sc.commitIndexCh,index)
	}
	sc.mu.Unlock()
}

//注意，与前三个RPC不同
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		OpType    :QueryType,
		ClientId  :args.ClientId,
		RequestId :args.RequestId,
		QueryNum  :args.Num,
	}
	sc.mu.Lock()
	//检查是否leader
	index,_,isleader := sc.rf.Start(op)
	if !isleader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		sc.mu.Unlock()
		return
	}
	ch, have := sc.commitIndexCh[index]
	if !have {
		ch = make(chan Op, 1)
		sc.commitIndexCh[index] = ch
	}
    sc.mu.Unlock()

	select {
		// 等待从 index 对应的 channel 中传来 Op
		case retOp := <-ch:
			if retOp.ClientId != args.ClientId||retOp.RequestId != args.RequestId {
				reply.Err = ErrWrongLeader
				reply.WrongLeader = true
			} else {
				reply.Err = OK
				reply.WrongLeader = false
				sc.mu.Lock()
				if retOp.QueryNum == -1 || retOp.QueryNum >= len(sc.configs){
					reply.Config = sc.configs[len(sc.configs)-1]
				}else{
					reply.Config = sc.configs[retOp.QueryNum]
				}
				sc.mu.Unlock()
			}
		// 没等到，超时了
		case <-time.After(sc.timeoutTime * time.Millisecond):
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
	}
	//
	sc.mu.Lock()
	ch, have = sc.commitIndexCh[index]
	if have {
		close(ch)
		delete(sc.commitIndexCh,index)
	}
	sc.mu.Unlock()
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applyListener(){
	for{
	    
        //DPrintf("aaaaaaaaaaaaaa")
		applyMsg := <- sc.applyCh
        if applyMsg.CommandValid {
            //DPrintf("AAAAAAAAAAAA")
            op := applyMsg.Command.(Op) //interface 类型转换
			index := int64(applyMsg.CommandIndex)
			requestId:=op.RequestId  
			//去重
			sc.mu.Lock()
			finishIndex,have := sc.finishSet[op.ClientId]
			isDuplicate:=false
			if have && requestId <= finishIndex{
                isDuplicate = true
			}
			sc.mu.Unlock()
            resOp := op
			if !isDuplicate{
				//如果不重复，要将结果应用于db
				sc.mu.Lock()
				sc.finishSet[op.ClientId] = op.RequestId
				//DPrintf("optype :%d\n",op.OpType)
                switch op.OpType{
				case JoinType:
					sc.configs = append(sc.configs,*sc.joinHandler(op.JoinServers))
					
				case LeaveType:
                    sc.configs = append(sc.configs,*sc.leaveHandler(op.LeaveGIDs))
				case MoveType:
					sc.configs = append(sc.configs,*sc.moveHandler(op.MoveShard,op.MoveGID))
				}
				sc.mu.Unlock()
			}
			sc.mu.Lock()
			//DPrintf("applyListen: key:%v, value %v\n",op.Key,op.Value)
			ch, exist := sc.commitIndexCh[int(index)]
			
	        if exist {
		        ch <- resOp
	        }
			sc.mu.Unlock()
		}
	}
}

func (sc *ShardCtrler) joinHandler(servers map[int][]string) *Config{

	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)

	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	for gid, serverLists := range servers {
		newGroups[gid] = serverLists
	}

	// GroupMap: groupId -> shards
	// 记录每个分组有几个分片(group -> shards可以一对多，也因此需要负载均衡，而一个分片只能对应一个分组）
	GroupMap := make(map[int]int)
	for gid := range newGroups {
		GroupMap[gid] = 0
	}

	// 记录每个分组存了多少分片
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			GroupMap[gid]++
		}
	}

	// 都没存自然不需要负载均衡,初始化阶段
	if len(GroupMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newGroups,
		}
	}

	//需要负载均衡的情况
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(GroupMap, lastConfig.Shards),
		//Shards: lastConfig.Shards,
		Groups: newGroups,
	}
}
func (sc *ShardCtrler) leaveHandler(gids []int) *Config{
	leaveMap := make(map[int]bool)
	for _, gid := range gids {
		leaveMap[gid] = true
	}

	lastConfig := sc.configs[len(sc.configs)-1]
	newGroups := make(map[int][]string)

	// 取出最新配置的groups组进行填充
	for gid, serverList := range lastConfig.Groups {
		newGroups[gid] = serverList
	}
	// 删除对应的gid的值
	for _, leaveGid := range gids {
		delete(newGroups, leaveGid)
	}
	// GroupMap: groupId -> shards
	// 记录每个分组有几个分片(group -> shards可以一对多，也因此需要负载均衡，而一个分片只能对应一个分组）
	GroupMap := make(map[int]int)
	newShard := lastConfig.Shards

	// 对groupMap进行初始化
	for gid := range newGroups {
		if !leaveMap[gid] {
			GroupMap[gid] = 0
		}
	}

	for shard, gid := range lastConfig.Shards {
		if gid != 0 {
			// 如果这个组在leaveMap中，则置为0
			if leaveMap[gid] {
				newShard[shard] = 0
			} else {
				GroupMap[gid]++
			}
		}

	}
	// 直接删没了
	if len(GroupMap) == 0 {
		return &Config{
			Num:    len(sc.configs),
			Shards: [10]int{},
			Groups: newGroups,
		}
	}
	return &Config{
		Num:    len(sc.configs),
		Shards: sc.loadBalance(GroupMap, newShard),
		//Shards : newShard,
		Groups: newGroups,
	}
}
func (sc *ShardCtrler) moveHandler(shard int ,gid int) *Config{
    lastConfig := sc.configs[len(sc.configs)-1]
	newConfig := Config{
		Num: len(sc.configs),
		Shards: [10]int{},
		Groups: map[int][]string{},
	}
	// 填充并赋值
	for tmpShard, tmpGid := range lastConfig.Shards {
		newConfig.Shards[tmpShard] = tmpGid
	}
	//只用改shards，不用负载均衡
	newConfig.Shards[shard] = gid

	for tmpGid, servers := range lastConfig.Groups {
		newConfig.Groups[tmpGid] = servers
	}

	return &newConfig
}
func sortGroupShard(GroupMap map[int]int) []int {
	length := len(GroupMap)

	gidSlice := make([]int, 0, length)

	// map转换成的slice
	for gid, _ := range GroupMap {
		gidSlice = append(gidSlice, gid)
	}

	// 将slice按gid大小顺序排
	for i := 0; i < length-1; i++ {
		for j := length - 1; j > i; j-- {

			if gidSlice[j] > gidSlice[j-1] {
				gidSlice[j], gidSlice[j-1] = gidSlice[j-1], gidSlice[j]
			}
		}
	}
	return gidSlice
}
func moreAllocations(length int, remainder int, i int) bool {

	// 这个目的是判断index是否在安排ave+1的前列:3、3、3、1 ,ave: 10/4 = 2.5 = 2,则负载均衡后应该是2+1,2+1,2,2
	if i < length-remainder {
		return true
	} else {
		return false
	}
}

func (sc *ShardCtrler) loadBalance(GroupMap map[int]int, lastShards [NShards]int) [NShards]int{
	length := len(GroupMap)
	ave := NShards / length
	remainder := NShards % length
	sortGids := sortGroupShard(GroupMap)

	// 先把负载多的部分free
	for i := 0; i < length; i++ {
		target := ave

		// 判断这个数是否需要更多分配，因为不可能完全均分，在前列的应该为ave+1
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}

		// 超出负载
		if GroupMap[sortGids[i]] > target {
			overLoadGid := sortGids[i]
			changeNum := GroupMap[overLoadGid] - target
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == overLoadGid {
					lastShards[shard] = 0
					changeNum--
				}
			}
			GroupMap[overLoadGid] = target
		}
	}

	// 为负载少的group分配多出来的group
	for i := 0; i < length; i++ {
		target := ave
		if !moreAllocations(length, remainder, i) {
			target = ave + 1
		}

		if GroupMap[sortGids[i]] < target {
			freeGid := sortGids[i]
			changeNum := target - GroupMap[freeGid]
			for shard, gid := range lastShards {
				if changeNum <= 0 {
					break
				}
				if gid == 0 {
					lastShards[shard] = freeGid
					changeNum--
				}
			}
			GroupMap[freeGid] = target
		}

	}
	return lastShards

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
	sc.commitIndexCh = make(map[int]chan Op)  
	sc.finishSet     = make(map[int64]int64) 
	sc.timeoutTime = 100

	go sc.applyListener()
	return sc
}
