package mr

import "fmt"
import "log"
import "os"
import "sort"
import "time"
import "strconv"
import "path/filepath"
import "encoding/json"
import "io/ioutil"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//	Reduce
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
    for{
        task := CallTask()
		switch task.TaskType{
		case Map:
			dealMapTask(&task,mapf)
		case Reduce:
            dealReduceTask(&task,reducef)
		case Exit:
			return
		case Wait:
			time.Sleep(5 * time.Second)
		}
	}
    
}

func CallTask() Task{
	args := ExampleArgs{}
	args.X = 1

	reply := Task{}
	call("Coordinator.DistributeTask", &args, &reply)
	return reply
}

func taskComplete(task *Task){
	reply := ExampleReply{Y:1}
	call("Coordinator.TaskCompleted", task, &reply)
	return
}

func dealMapTask(task *Task,mapf func(string, string) []KeyValue){
	file, err := os.Open(task.InputFile)
	if err != nil {
		log.Fatalf("cannot open %v", task.InputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.InputFile)
	}
	file.Close()
	intermediates := mapf(task.InputFile, string(content))

	buffer := make([][]KeyValue,task.ReducerNum)
	for _,intermediate := range intermediates {
		slot := ihash(intermediate.Key)%task.ReducerNum
		buffer[slot] = append(buffer[slot],intermediate)
	}
	strOutPut := make([]string,0)
	for i:=0;i<task.ReducerNum;i++{
		strOutPut = append(strOutPut,writeToLocal(task.TaskId,i,&buffer[i])) 
	}
	task.Intermediates = strOutPut
	taskComplete(task)
}

func dealReduceTask(task *Task,reducef func(string, []string) string){
    intermediate :=*readFromLocal(task.Intermediates)
	sort.Sort(ByKey(intermediate))

	dir,_ := os.Getwd()
    tempFile,err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil{
		log.Fatal("Failed to create tmp file",err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-out-%d",task.TaskId)
	os.Rename(tempFile.Name(),outputName)
	task.Output = outputName
	taskComplete(task)
}
func writeToLocal(taskId int,reducerId int,buffer *[]KeyValue) string{
    dir,_ := os.Getwd()
	tempFile,err := ioutil.TempFile(dir, "mr-tmp-*")
    if err != nil{
		log.Fatal("Failed to create tmp file",err)
	}
	enc := json.NewEncoder(tempFile)
	for _,kv := range *buffer{
        if err := enc.Encode(&kv);err!=nil{
			log.Fatal("Failed to write kv pair",err)
		}
	}
	tempFile.Close()
    outputName := fmt.Sprintf("mr-tmp-%d-%d",taskId,reducerId)
    os.Rename(tempFile.Name(),outputName)
	return filepath.Join(dir,outputName)
}
func readFromLocal(files []string) *[]KeyValue{
	kva := []KeyValue{}
	for _,filepath:=range files{
		file,err:=os.Open(filepath)
		if err!=nil{
			log.Fatal("Failed to open file"+filepath,err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv);err!=nil{
				break;
			}
			kva = append(kva,kv)
		}
		file.Close()
	}
	return &kva
}

func dealTask(task *Task,mapf func(string, string) []KeyValue,reducef func(string, []string) string){
	file, err := os.Open(task.InputFile)
	if err != nil {
		log.Fatalf("cannot open %v", task.InputFile)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.InputFile)
	}
	file.Close()
	intermediate := mapf(task.InputFile, string(content))
	sort.Sort(ByKey(intermediate))
	
	oname := "mr-tmp-"+strconv.Itoa(task.TaskId)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
