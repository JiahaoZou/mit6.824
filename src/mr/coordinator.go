package mr

import "log"
import "net"
import "os"
import "sync"
import "time"
import "net/rpc"
import "net/http"

type TaskManagerState int

const (
	Idle       = iota
	InProgress
	Completed
)

type State int

const (
	Map    = iota
	Reduce
	Exit
	Wait
)

type Coordinator struct {
	TaskChannel       chan *Task
    TaskMap           map[int]*TaskManager
	MasterPharse      State
	ReducerNum        int
	InputFile         []string
	Intermediates     [][]string
}
type TaskManager struct{
    TaskStatus  TaskManagerState
	StartTime   time.Time
	TaskPtr     *Task
}
type Task struct {
	TaskType    State
	InputFile   string
	ReducerNum  int
	TaskId      int
	Intermediates []string
	Output        string
}

var mu sync.Mutex

func (c *Coordinator) DistributeTask(args *ExampleArgs, reply *Task) error{
    mu.Lock()
	defer mu.Unlock()
	if len(c.TaskChannel)>0 {
		*reply = *<- c.TaskChannel
		c.TaskMap[reply.TaskId].StartTime = time.Now()
		c.TaskMap[reply.TaskId].TaskStatus = InProgress
	}else if c.MasterPharse == Exit{
		*reply = Task{TaskType: Exit}
	}else{
		*reply = Task{TaskType: Wait}
	}
	return nil
}

func (c *Coordinator) TaskCompleted(args *Task, reply *ExampleReply) error{
    
	mu.Lock()
	defer mu.Unlock()
	if c.MasterPharse == Exit || c.TaskMap[args.TaskId].TaskStatus==Completed || c.MasterPharse != args.TaskType{
		return nil
	}else{
		c.TaskMap[args.TaskId].TaskStatus = Completed
		go c.processTaskResult(args)
	}
    return nil 
}

func (c *Coordinator) processTaskResult(task *Task){
    mu.Lock()
	defer mu.Unlock()
	switch task.TaskType{
	case Map:
		for reduceTaskId,filePath:=range task.Intermediates{
			c.Intermediates[reduceTaskId] = append(c.Intermediates[reduceTaskId],filePath)
		}
		if c.isAllCompleted(){
			c.createReduceTask()
			c.MasterPharse = Reduce
		}
	case Reduce:
		if c.isAllCompleted(){
			c.MasterPharse = Exit
		}
	}
}

func (c *Coordinator) isAllCompleted() bool{
	for _,manager := range c.TaskMap{
		if manager.TaskStatus!=Completed{
			return false
		}
	}
	return true
}
// the RPC argument and reply types are defined in rpc.go.
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	mu.Lock()
	defer mu.Unlock()
    if c.MasterPharse==Exit{
		ret = true
	}

	return ret
}

func (c *Coordinator)createMapTask() {
	mu.Lock()
	defer mu.Unlock()
	for id, filename := range c.InputFile{
		task := Task{
			TaskType   : Map,
	        InputFile  : filename,
	        TaskId     : id,
	        ReducerNum : c.ReducerNum,
		}
		c.TaskMap[id] = &TaskManager{
			TaskStatus : Idle,
	        TaskPtr    : &task,
		}
		c.TaskChannel <- &task
	}
}

func (c *Coordinator)createReduceTask() {
	c.TaskMap = make(map[int]*TaskManager)
    for id,files := range c.Intermediates{
		task := Task{
			TaskType   : Reduce,
	        TaskId     : id,
	        ReducerNum : c.ReducerNum,
            Intermediates: files,
		}
		c.TaskMap[id] = &TaskManager{
			TaskStatus : Idle,
	        TaskPtr    : &task,
		}
		c.TaskChannel <- &task
	}
}

func Max(a int, b int) int{
	if a<b {
		return b
	}
	return a
}

func (c *Coordinator)catchTimeout(){
	for{
		time.Sleep(5*time.Second)
		mu.Lock()
		if c.MasterPharse==Exit{
			mu.Unlock()
			return
		}
		for _, masterTask := range c.TaskMap{
			if masterTask.TaskStatus==InProgress && time.Now().Sub(masterTask.StartTime)>10*time.Second{
				c.TaskChannel <- masterTask.TaskPtr
				masterTask.TaskStatus = Idle
			}
		}
		mu.Unlock()
	}
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskChannel: make(chan *Task, Max(len(files),nReduce)),
		TaskMap    : make(map[int]*TaskManager),  
		Intermediates: make([][]string,nReduce),       
		MasterPharse: Map,
	    ReducerNum : nReduce,
	    InputFile  : files,
	}
    
	c.createMapTask()
	
	c.server()
	go c.catchTimeout()
	return &c
}
