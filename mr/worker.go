package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func getKVPairs(mapf func(string, string) []KeyValue, filename string) []KeyValue {
	file, err := os.Open(filename)

	if err != nil {
		log.Fatalf("cannot open %v", filename)
		return nil
	}

	content, err := ioutil.ReadAll(file)

	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return nil
	}

	file.Close()
	return mapf(filename, string(content))
}

func mapper(mapf func(string, string) []KeyValue, mapJob *MapJob) {
	kva := getKVPairs(mapf, mapJob.InputFile)
	if kva == nil {
		// the coordinator will mark it as dead
		return
	}

	sort.Sort(ByKey(kva))

	partitionedKva := make([][]KeyValue, mapJob.ReducerCount)

	for _, v := range kva {
		partitionedKey := ihash(v.Key) % mapJob.ReducerCount
		partitionedKva[partitionedKey] = append(partitionedKva[partitionedKey], v)
	}

	intermediateFiles := make([]string, mapJob.ReducerCount)
	for i := 0; i < mapJob.ReducerCount; i++ {
		intermediateFile := fmt.Sprintf("mr-%v-%v", mapJob.MapJobNum, i)
		intermediateFiles[i] = intermediateFile
		ofile, _ := os.Create(intermediateFile)

		b, err := json.Marshal(partitionedKva[i])
		if err != nil {
			fmt.Println("Marshal error: ", err)
		}
		ofile.Write(b)
		ofile.Close()
	}
	reportMapTaskComplete(mapJob.InputFile, intermediateFiles)
}

func reducer(reducef func(string, []string) string, reduceJob *ReduceJob) {
	files := reduceJob.IntermediateFiles
	intermediate := []KeyValue{}

	for _, f := range files {
		data, err := ioutil.ReadFile(f)
		if err != nil {
			fmt.Println("read error: ", err.Error())
		}
		var input []KeyValue
		err = json.Unmarshal(data, &input)
		if err != nil {
			fmt.Println("Unmarshal error: ", err.Error)
		}

		intermediate = append(intermediate, input...)
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", reduceJob.ReducerNum)
	tempFile, err := ioutil.TempFile(".", oname)
	if err != nil {
		fmt.Println("Error creating temp file")
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

		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	os.Rename(tempFile.Name(), oname)
	reportReduceTaskComplete(reduceJob.ReducerNum)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	job_finished := false

	// job's not finished i don't think so - bald man who wore 8 & 24.
	for !job_finished {
		reply := requestJob()

		if reply == nil {
			return
		}

		if reply.Done {
			job_finished = true
			continue
		}

		if reply.MapJob != nil {
			fmt.Printf("I am worker %v and I have been assigned %v\n", os.Getpid(), reply.MapJob.InputFile)
			mapper(mapf, reply.MapJob)
		} else if reply.ReduceJob != nil {
			reducer(reducef, reply.ReduceJob)
		}
	}
}

func reportMapTaskComplete(inputFile string, intermediateFiles []string) {
	args := ReportMapTaskArgs{}
	args.InputFile = inputFile
	args.IntermediateFiles = intermediateFiles
	args.Pid = os.Getpid()

	reply := ReportReply{}

	call("Coordinator.MarkMapTaskComplete", &args, &reply)
	fmt.Println(reply.Message)
}

func reportReduceTaskComplete(reducerNum int) {
	args := ReportReduceTaskArgs{}
	args.Pid = os.Getpid()
	args.ReducerNum = reducerNum

	reply := ReportReply{}

	call("Coordinator.MarkReduceTaskComplete", &args, &reply)
	fmt.Println(reply.Message)
}

func requestJob() *RequestTaskReply {
	args := RegisterArgs{}

	args.Pid = os.Getpid()

	reply := new(RequestTaskReply)

	ok := call("Coordinator.RequestJob", &args, reply)

	if ok {
		return reply
	} else {
		return nil
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
