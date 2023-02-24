package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"

	"time"
	"math/rand"
	//"fmt"
)


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

type Entry struct{
	Term  int
	Index int
	Command interface{}
}
//
// A Go object implementing a single Raft peer.
//
const (
	follower  = iota
	candidate
    leader    
)
const (
	AppNormal     = iota                   // 追加正常
	AppOutOfDate                           // 追加过时
	AppKilled                              // Raft程序终止
	AppCommitted                           // 追加的日志已经提交 (2B
	Mismatch                               // 追加不匹配 (2B
)


type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state         int
	currentTerm   int 
    votedFor      int
	logs          []Entry

	commitIndex   int
	lastApplied   int

	nextIndex     []int
	matchIndex    []int

	heartBeat     time.Duration
    electionTime  time.Time

	appendEntryCh chan *Entry

	applyCh       chan ApplyMsg
	applyCond     *sync.Cond

	//2D
	lastIncludeIndex int
	lastIncludeTerm  int
	msgmu         sync.Mutex
}

func max(a int , b int) int {
	if a >= b {
		return a
	}else{
		return b
	}
}
func min(a int, b int) int {
	if a >= b{
		return b
	}else {
		return a
	}
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	if rf.state==leader{
		isleader = true
	}else{
		isleader = false
	}

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//数据持久化，具体的实现由persister实现，是将数据存储到本地磁盘上
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []Entry
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil ||
	   d.Decode(&logs) !=nil||
	   d.Decode(&lastIncludeIndex)!=nil||
	   d.Decode(&lastIncludeTerm)!=nil{
	   
	}else {
	   rf.currentTerm = currentTerm
	   rf.votedFor = votedFor
	   rf.logs = logs
	   rf.lastIncludeIndex = lastIncludeIndex
	   rf.lastIncludeTerm = lastIncludeTerm
	   rf.commitIndex = lastIncludeIndex
	   rf.lastApplied = lastIncludeIndex
	}
	/*
	if rf.lastIncludeIndex > 0 {
		var snapApplyMsg ApplyMsg
		snapApplyMsg.SnapshotValid = true
		snapApplyMsg.SnapshotIndex = rf.lastIncludeIndex
		snapApplyMsg.SnapshotTerm = rf.lastIncludeTerm
		snapApplyMsg.Snapshot = rf.persister.ReadSnapshot()
	    rf.applyCh<-snapApplyMsg
	}*/
}

type InstallSnapshotArgs struct{
    Term      int
	LeaderId  int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data      []byte
}

type InstallSnapshotReply struct{
    Term     int
}

func (rf *Raft) GetRaftStateSize() int {

	return rf.persister.RaftStateSize()
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).


	return true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	rf.mu.Lock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	rf.state = follower
	rf.votedFor = -1
	//rf.persist()
	rf.resetElectionTimer()

	if rf.lastIncludeIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}

    
	//截取follower的日志，将快照包含的全部丢弃
    if rf.getLastIndex() < args.LastIncludeIndex {
		rf.logs = rf.logs[0:1]
	}else{
		for cutIndex, val := range rf.logs {
			if val.Index == args.LastIncludeIndex {
				rf.logs = rf.logs[cutIndex+1:]
				var tempLogArray []Entry = make([]Entry, 1)
				// make sure the log array is valid starting with index=1
				rf.logs = append(tempLogArray, rf.logs...)
			}
		}
	}
	//正式处理
	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastIncludeTerm = args.LastIncludeTerm 
    //既然一次性提交了这么多日志，需要更新commit和applied
	if rf.lastApplied < rf.lastIncludeIndex {
		rf.lastApplied = rf.lastIncludeIndex
	}
	if rf.commitIndex < rf.lastIncludeIndex {
		rf.commitIndex = rf.lastIncludeIndex
	}
	//持久化
    w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, args.Data)
    //提交快照
	var snapApplyMsg ApplyMsg
	snapApplyMsg.SnapshotValid = true
	snapApplyMsg.SnapshotIndex = args.LastIncludeIndex
	snapApplyMsg.SnapshotTerm = args.LastIncludeTerm
	snapApplyMsg.Snapshot = args.Data
    //fmt.Printf("install snapshot, server:%d, lastIndex :%d\n",rf.me,args.LastIncludeIndex)
    
	rf.mu.Unlock()
	rf.applyCh <- snapApplyMsg
	
	
}

// index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
// 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//index 表示调用者希望生成的快照中最大的index
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果下标大于自身的提交，说明没被提交不能安装快照，如果自身快照点大于index说明不需要安装
	//fmt.Println("[Snapshot] commintIndex", rf.commitIndex)
	if rf.lastIncludeIndex >= index || index > rf.commitIndex {
		return
	}
	// 更新快照日志
	for cutIndex, val := range rf.logs {
		if val.Index == index {
			rf.lastIncludeIndex = index
			rf.lastIncludeTerm = val.Term
			rf.logs = rf.logs[cutIndex+1:]
			var tempLogArray []Entry = make([]Entry, 0)
			tempLogArray = append(tempLogArray,Entry{})
			// make sure the log array is valid starting with index=1
			rf.logs = append(tempLogArray, rf.logs...)
		}
	}

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	// 持久化快照信息
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

func (rf *Raft) leaderSendSnapShot(server int, args *InstallSnapshotArgs){
	reply := InstallSnapshotReply{}
	ok := rf.sendSnapShot(server, args, &reply)
	if !ok{
       return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
    if reply.Term > rf.currentTerm{
		rf.setNewTerm(reply.Term)
		return
	}
	if reply.Term == rf.currentTerm {
		rf.nextIndex[server] = max(rf.nextIndex[server],args.LastIncludeIndex+1)
        rf.matchIndex[server] = max(rf.matchIndex[server],args.LastIncludeIndex)
	}
}
//下标变换的封装函数
func (rf *Raft) getLastIndex() int{
    return rf.lastIncludeIndex + len(rf.logs) - 1
}
func (rf *Raft) getLastTerm() int{
    if rf.lastIncludeTerm == 0 || len(rf.logs)>1 {
		return rf.logs[len(rf.logs)-1].Term
	}else{
		return rf.lastIncludeTerm
	}
}
func (rf *Raft) getposByIndex(index int) int{
	if rf.lastIncludeIndex == 0{
		return index
	}else if len(rf.logs)==1{
		return 0
	}else if index<=rf.lastIncludeIndex{
        return 0
	}else{
		return index - rf.lastIncludeIndex 
	}
}
//按传入的下标找
func (rf *Raft) restoreLog(index int) Entry{
	if rf.lastIncludeIndex == 0{
		return rf.logs[index]
	}else if index <= rf.lastIncludeIndex{
		return rf.logs[0]
	}else{
		return rf.logs[index-rf.lastIncludeIndex]
	}
}
func (rf *Raft) restoreLogTerm(index int) int{
    if index <= rf.lastIncludeIndex{
		return rf.lastIncludeTerm
	}else{
		return rf.restoreLog(index).Term
	}
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}
type AppendEntresArgs struct {
	Term         int
	LeaderId  int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int //leader已经提交的日志号，收到了后follewer可以也把自己的该日志提交，
	                 //当集群中大多数节点都已经提交了此日志，则完成了集群提交状态
}
type AppendEntresReply struct {
	Term        int
	Success     bool
	State       int
    XTerm       int
	XIndex      int
	XLen        int
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
    if args.Term>rf.currentTerm {       //all Server rule
        rf.setNewTerm(args.Term)        //语句1，任期大于自己，自己肯定是不能竞选的，重置竞选时间放到之后
		rf.persist()
	}
	reply.Term = rf.currentTerm
    if args.Term<rf.currentTerm{
		reply.VoteGranted = false
		return
	}

	myLastLog := rf.logs[len(rf.logs)-1]
	upToDate := args.LastLogTerm>myLastLog.Term ||
    (args.LastLogTerm==myLastLog.Term&&args.LastLogIndex>=myLastLog.Index)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId)&&upToDate{ 
        reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.persist()
		rf.resetElectionTimer()        //更新选举倒计时
		
	}else{
		reply.VoteGranted = false
	}

	
}
//follower接收数据包的RPC，注意，心跳包和数据包的实现并无本质区别，只是携带的log数量不一样，因此在
//处理数据包时也不需要额外的判断是否是心跳包，统一处理即可
func (rf *Raft) AppendEntres(args *AppendEntresArgs, reply *AppendEntresReply){
	
	rf.mu.Lock()
	defer rf.mu.Unlock()
    reply.Success = false
	reply.Term =  rf.currentTerm
	reply.State = AppNormal
	if rf.killed(){
        reply.State = AppKilled  
		return
	}
	if args.Term > rf.currentTerm{
		rf.setNewTerm(args.Term)
		rf.resetElectionTimer() 
		rf.persist()
		
	}
    if args.Term < rf.currentTerm{
        return 
	}
    
	rf.resetElectionTimer()    //收到心跳后立即重置选举倒计时，防止触发选举
	rf.votedFor = -1           //投票信息清空，为了下一次可能的选举
	rf.state = follower
    rf.persist()
	if rf.getLastIndex() < args.PrevLogIndex{ //index太大
		reply.State = Mismatch 
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.getLastIndex()+1
		return
	}
	//注意，由之前一个if语句我们可以确定，走到这一步，args.PrevLogIndex<=rf.lastindex
	//所以只会由两种情况：1，follower进行过快照，即rf.lastIncludeIndex>0 2。follower没有进行过快照

	/*
            if reply.XTerm == -1 {//index不匹配
				rf.nextIndex[server] = reply.XLen
			}else{ //term不匹配
				lastLogIndex := rf.findLastLogInTerm(reply.XTerm)
				if lastLogIndex > 0{
                    rf.nextIndex[server] = lastLogIndex
				}else{
				    rf.nextIndex[server] = reply.XIndex
				}
			}

	*/
    if rf.lastIncludeIndex>0 && args.PrevLogIndex < rf.lastIncludeIndex{
    //让leader从lastIncludeIndex+1处传
		reply.State = Mismatch 
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.lastIncludeIndex+1
		return
	}
	//到这里可以保证 args.PrevLogIndex >= rf.lastIncludeIndex 且 rf.lastIncludeIndex<=rf.getLastIndex()
	if rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm{ //Term不匹配
		
		reply.State = Mismatch 
		xTerm := rf.restoreLogTerm(args.PrevLogIndex)
		for xIndex:=args.PrevLogIndex; xIndex>rf.lastIncludeIndex;xIndex--{
			//找上一个Term的最后一个index
			if rf.restoreLogTerm(xIndex-1)!=xTerm{
				reply.XIndex = xIndex
				break
			}
		}
		reply.XTerm = xTerm
		reply.XLen = rf.getLastIndex()+1
		return
	}
		   /********正确匹配后************/
	//因为args里携带的Entries可能不止一个entry，且follower可能已经拥有了前几个Entry，所以找到
	//entries里第一个与follower已经拥有的log不匹配的项
	/*
	aa := false
	bb := false
	if rf.lastIncludeIndex==9{
		if len(args.Entries)!=0{
			fmt.Printf("argsstartindex: %d end: %d\n",args.Entries[0].Index,args.Entries[len(args.Entries)-1].Index)
		}
	}
	*/
	for idx,entry := range args.Entries{
		if entry.Index <= rf.getLastIndex() && rf.restoreLogTerm(entry.Index) != entry.Term{
		   //进入条件，没有到rf的logs的最后，就已经出现了Term不匹配
		   //entry.Index不匹配，将entry.Index之后的全部丢弃
		   rf.logs = rf.logs[:rf.getposByIndex(entry.Index)] 
		   rf.persist()
		   //aa = true
		   //此时len（rf.logs） == entry.Index - 1
		} 

		if entry.Index > rf.getLastIndex(){
		   //fmt.Printf("bbtrue:   %d, size: %d, lastIndex:%d\n",rf.getLastIndex(),len(rf.logs)-1,rf.logs[len(rf.logs)-1].Index)
		   rf.logs = append(rf.logs,args.Entries[idx:]...)
		   rf.persist()
		   //bb = true
		   break;
		} 
	}
	
	reply.Success = true
    //依靠leader心跳来触发follower的提交
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = max(rf.commitIndex,min(args.LeaderCommit,rf.getLastIndex()))
		rf.apply()
	}
	/*
	if rf.lastIncludeIndex==9{
		fmt.Printf("server: %d lastIndex: %d ,lastpos:%d, commit: %d, applied: %d,command:%v,INDEX:%d,aa:%v.bb%v\n",rf.me,rf.getLastIndex(),len(rf.logs)-1,
		                    rf.commitIndex,rf.lastApplied,rf.restoreLog(10).Command,rf.restoreLog(10).Index,aa,bb)
		
	} 
	*/
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntres(server int, args *AppendEntresArgs, reply *AppendEntresReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntres", args, reply)
	return ok
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool{
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
//
//向leader发log，由raft上层调用
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
    rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2B).
    if rf.killed(){
		return index,rf.currentTerm,false
		
	}
	if rf.state != leader{
		return index,rf.currentTerm,false
	}
    index = rf.getLastIndex()+1 
	term  = rf.currentTerm
    
	log := Entry{
		Command : command,
		Index   : index,
		Term    : term,
	}
	rf.logs =append(rf.logs,log)
	rf.persist()
	rf.appendEntres(false)
    
	return index, term, isLeader
}

//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
// 定时，按照设定的心跳时间来定时触发leader的传心跳包，或是检查是否超出了选举倒计时，开始作为候选人参与选举。
func (rf *Raft) ticker() {
	for rf.killed() == false {
        time.Sleep(rf.heartBeat)
		rf.mu.Lock()
        if rf.state == leader{
			rf.resetElectionTimer()     
			rf.appendEntres(true)
		}
		if time.Now().After(rf.electionTime){
			rf.leaderElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) appendEntres(isHeartBeat bool){
	
    lastLogIndex := rf.getLastIndex() 
	for id,_ := range rf.peers {
		if id == rf.me {
			continue
		}
		if rf.nextIndex[id] <=0 {
			rf.nextIndex[id] = 1
		}
		if lastLogIndex >= rf.nextIndex[id] || isHeartBeat || rf.lastIncludeIndex>=rf.nextIndex[id]{ 
			nextIndex := rf.nextIndex[id]
			if lastLogIndex + 1 < nextIndex{
				nextIndex = lastLogIndex
			}
			if nextIndex > rf.lastIncludeIndex{
				//不发快照
				//发appendEntry
				
				args := AppendEntresArgs{
					Term         : rf.currentTerm,
					LeaderId     : rf.me,
					PrevLogIndex : nextIndex-1,
					PrevLogTerm  : rf.restoreLogTerm(nextIndex-1),
					Entries      : make([]Entry,lastLogIndex-nextIndex+1),
					LeaderCommit : rf.commitIndex,
				}
				/*
				if nextIndex == 10{
					fmt.Printf("size: %d, start: %d, end: %d, lastindex: %d\n",lastLogIndex-nextIndex+1 ,rf.getposByIndex(nextIndex), len(rf.logs)-1,rf.getLastIndex())
	                			
				}
				*/
				copy(args.Entries,rf.logs[rf.getposByIndex(nextIndex):]) 
				
				go rf.sendEntres(id,&args)
			}else{
				//nextIndex太小，发快照
                args := InstallSnapshotArgs{
					Term      : rf.currentTerm,
					LeaderId  : rf.me,
					LastIncludeIndex : rf.lastIncludeIndex,
					LastIncludeTerm  : rf.lastIncludeTerm,
					Data      : rf.persister.ReadSnapshot(),
				}
				go rf.leaderSendSnapShot(id,&args)
			}
			
		}
	}

}

func (rf *Raft) sendEntres(server int, args *AppendEntresArgs){
	reply := AppendEntresReply{}
	ok := rf.sendAppendEntres(server,args,&reply)
	if ok!=true{
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.State == AppKilled{
		return
	}
	if reply.Term>rf.currentTerm {
		rf.setNewTerm(reply.Term)      //主动退位，改变自己为follower
		rf.persist()
		return
	}
	if args.Term == rf.currentTerm {
		
		if reply.Success{
			match := args.PrevLogIndex + len(args.Entries)
			next := match + 1
			rf.nextIndex[server] = max(rf.nextIndex[server],next)
			rf.matchIndex[server] = max(rf.matchIndex[server],match)
		}else if reply.State == Mismatch{
			if reply.XTerm == -1 {//index不匹配
				rf.nextIndex[server] = reply.XLen
			}else{ //term不匹配
				lastLogIndex := rf.findLastLogInTerm(reply.XTerm)
				if lastLogIndex > 0{
                    rf.nextIndex[server] = lastLogIndex
				}else{
				    rf.nextIndex[server] = reply.XIndex
				}
			}
			//fmt.Printf("leader id : %d , nextIndex : %d, server: %d\n",rf.me,rf.nextIndex[server], server)
		}else if rf.nextIndex[server] >1 {
			rf.nextIndex[server]--
		}
		rf.leaderCommit()
	}
	
}

func (rf *Raft) findLastLogInTerm(term int) int{
	len := len(rf.logs)
	for i:=len-1 ; i>0 ; i-- {
        log := rf.logs[i]
		if log.Term == term {
            return log.Index
		}
	} 
	return 0
}
func (rf *Raft) leaderCommit(){
    if rf.state != leader {
		return
	}

	for n := rf.commitIndex + 1; n<=rf.getLastIndex();n++{
		//未提交日志任期与当前任期不符的，不能贸然提交，只有当前任期日志可以提交了后，其前面的日志均可提交
		if rf.restoreLogTerm(n) != rf.currentTerm{
			continue
		}
		counter := 1
		for id,_ := range rf.peers {
			if id != rf.me && rf.matchIndex[id] >= n{
				counter++
			}
			if counter > len(rf.peers)/2 {
				rf.apply()
				rf.commitIndex = n
				break
			}
		}
	}
}

func (rf *Raft) leaderElection(){
	rf.currentTerm++
	rf.state = candidate
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()
	term := rf.currentTerm
	voteCounter  := 1
	lastLog := rf.getLastIndex()
	lastTerm := rf.getLastTerm()

	args := RequestVoteArgs{
		Term         : term,
	    CandidateId  : rf.votedFor,
	    LastLogIndex : lastLog,
	    LastLogTerm  : lastTerm,
	}

	
	for id,_ := range rf.peers{
        if id!=rf.me{
			go rf.condidateRequestVote(id,&args,&voteCounter)
		}
	}
}

func (rf *Raft) condidateRequestVote(serverId int, args *RequestVoteArgs, voteCounter *int){
    reply := RequestVoteReply{}

	ok := rf.sendRequestVote(serverId,args,&reply)
	if ok!=true{
       return 
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state!=candidate{      //在竞选途中放弃或是已经成为leader，之后的选票都没有意义了
		return
	}
	if reply.Term < rf.currentTerm { //说明已经死亡，或是旧的返回值
		return
	}
	if reply.VoteGranted == true{
		*voteCounter++
	}
	if reply.Term > rf.currentTerm {   //对方的term比自己大，放弃竞选，并重置自己的竞选投票和竞选时间
		rf.setNewTerm(rf.currentTerm)
		rf.persist()
		rf.resetElectionTimer()
	}
    if *voteCounter > len(rf.peers)/2&&
	    rf.currentTerm==args.Term&& 
		rf.state == candidate {  //防止重复触发变更为leader
		rf.state = leader
		lastLogIndex := rf.getLastIndex()
		for i,_ := range rf.peers {
			rf.nextIndex[i] = lastLogIndex + 1 
			rf.matchIndex[i] = 0
		}
		rf.appendEntres(true)    //成为leader后立即广播一次心跳
	}
}

//重置选举倒计时
func (rf *Raft)resetElectionTimer(){
	//fmt.Printf("Aaaa\n")
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
    t := time.Now()
	electionTimeout := time.Duration(800+r.Intn(400))*time.Millisecond
	rf.electionTime = t.Add(electionTimeout)
}
//更新新任期，在选举失败或是follower接到leaderrpc时更新任期
func (rf *Raft)setNewTerm(term int){
	if term > rf.currentTerm || rf.currentTerm==0{
		rf.state = follower
		rf.currentTerm = term
		rf.votedFor = -1
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.state = follower
	rf.votedFor = -1
	rf.logs = make([]Entry, 0)
	rf.logs = append(rf.logs,Entry{0,0,nil})
	rf.commitIndex = 0  //第0条是空的，entry的index是从0开始
	rf.lastApplied = 0 
	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0
	rf.nextIndex = make([]int,len(rf.peers))
	rf.matchIndex = make([]int,len(rf.peers))

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.heartBeat = 50*time.Millisecond
    rf.resetElectionTimer()
	// Your initialization code here (2A, 2B, 2C).
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
    
	// start ticker goroutine to start elections
	go rf.ticker()
    go rf.applier()

	return rf
}

func (rf *Raft) apply(){
	rf.applyCond.Broadcast()
}

func (rf *Raft) applier(){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed(){
		//log下标从1开始，第0个为空log
		if rf.commitIndex > rf.lastApplied && rf.getLastIndex() > rf.lastApplied && rf.lastApplied >= rf.lastIncludeIndex{
			rf.lastApplied++;  //此时已指向未提交的第一条
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.restoreLog(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			rf.mu.Unlock()
			
			rf.applyCh <- applyMsg
			
			rf.mu.Lock()
		}else{
			rf.applyCond.Wait()
		}
	}
}
