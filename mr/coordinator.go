package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

/*
* The JobStatus struct is responsible for storing the
* time a job started and the state of the job.
* The time that a job started is used to find stragglers
* and the state is used to incrementally push updates to reduce tasks that are in progress
* and keep track of the status of the job.
* state = either("idle", "running [map|reduce]", "")
 */
type JobStatus struct {
	startTime int64
	state     string
}

/* The coordinator struct is what holds all the data for the MapReduce
 * computation.
 */
type Coordinator struct {
	workers           map[int]string       // workers are associated with thier pid
	nReduce           int                  // the number of reduce tasks
	mapStatus         map[string]JobStatus // the status of all the map tasks that are passed in
	reducerStatus     []JobStatus          // the status of all the reduce tasks
	mapId             int                  // id of the map task that has been instantiated
	intermediatefiles map[int][]string     // for the i-th reduce task the intermediate files stores the location of the computations of the j-th map task if it has keys for that reduce task
	mu                sync.Mutex           // the lock
}

func (c *Coordinator) mapPhaseFinished() bool {
	ret := true
	for _, status := range c.mapStatus {
		if status.state != "complete" {
			ret = false
		}
	}
	return ret
}

func (c *Coordinator) findPendingMapTask() *MapJob {
	for filename, job_status := range c.mapStatus {
		if job_status.state == "pending" {
			// if the job is pending then we know that it can be assigned
			mapJob := &MapJob{}
			mapJob.InputFile = filename
			mapJob.MapJobNum = c.mapId
			c.mapId++
			mapJob.ReducerCount = c.nReduce
			c.mapStatus[filename] = JobStatus{startTime: time.Now().Unix(), state: "running"}
			return mapJob
		}
	}
	return nil
}

func (c *Coordinator) findPendingReduceTask() *ReduceJob {
	for i := range c.reducerStatus {
		if c.reducerStatus[i].state == "pending" {
			// the ith reduce task is pending
			reduceJob := new(ReduceJob)
			reduceJob.IntermediateFiles = c.intermediatefiles[i]
			reduceJob.ReducerNum = i
			startTime := time.Now().Unix()
			c.reducerStatus[i] = JobStatus{startTime: startTime, state: "running"}
			return reduceJob
		}
	}
	return nil
}

func (c *Coordinator) RequestJob(args *RegisterArgs, reply *RequestTaskReply) error {
	c.mu.Lock()
	if c.workers[args.Pid] == "" {
		c.workers[args.Pid] = "idle"
	}

	// the map phase happens before the reduce phase so we do the following.
	mapJob := c.findPendingMapTask()
	if mapJob != nil {
		// we found a map task.
		reply.MapJob = mapJob
		reply.Done = false
		c.workers[args.Pid] = "running map"
		c.mu.Unlock()
		return nil
	}

	if !c.mapPhaseFinished() {
		reply.MapJob = mapJob
		reply.Done = false
		c.mu.Unlock()
		return nil
	}

	reduceJob := c.findPendingReduceTask()
	if reduceJob != nil {
		reply.ReduceJob = reduceJob
		reply.Done = false
		c.workers[args.Pid] = "running reduce"
		c.mu.Unlock()
		return nil
	}

	c.mu.Unlock()
	reply.Done = c.Done()
	return nil
}

func (c *Coordinator) MarkMapTaskComplete(args *ReportMapTaskArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.workers[args.Pid] = "idle"
	c.mapStatus[args.InputFile] = JobStatus{startTime: -1, state: "complete"}
	for r := 0; r < c.nReduce; r++ {
		c.intermediatefiles[r] = append(c.intermediatefiles[r], args.IntermediateFiles[r])
	}

	reply.Message = fmt.Sprintf("I am proud. Good job mapper %v!👏 %v", args.InputFile, args.Pid)
	return nil
}

func (c *Coordinator) MarkReduceTaskComplete(args *ReportReduceTaskArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reducerStatus[args.ReducerNum] = JobStatus{startTime: -1, state: "complete"}
	c.workers[args.Pid] = "idle"
	reply.Message = fmt.Sprintf("Good job %v reducer(pid=%v)✅", args.ReducerNum, args.Pid)
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := true
	// the job is finished once all the reduce tasks are marked as complete
	for _, r := range c.reducerStatus {
		if r.state != "complete" {
			return false
		}
	}
	return ret
}

func (c *Coordinator) StartTicker() {
	ticker := time.NewTicker(10 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				if c.Done() {
					return
				}
				c.CheckDeadWorker()
			}
		}
	}()
}

func (c *Coordinator) CheckDeadWorker() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, ms := range c.mapStatus {
		if ms.state == "running" {
			now := time.Now().Unix()
			if ms.startTime > 0 && now > (ms.startTime+10) {
				c.mapStatus[i] = JobStatus{startTime: -1, state: "pending"}
				continue
			}
		}
	}

	for i, rs := range c.reducerStatus {
		if rs.state == "running" {
			now := time.Now().Unix()
			if rs.startTime > 0 && now > (rs.startTime+10) {
				c.reducerStatus[i] = JobStatus{startTime: -1, state: "pending"}
				continue
			}
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce

	c.mapId = 0
	c.intermediatefiles = make(map[int][]string)

	c.mapStatus = make(map[string]JobStatus)
	c.reducerStatus = make([]JobStatus, nReduce)
	c.workers = make(map[int]string)
	// init the map statuses to be idle for all the map tasks that we need to do
	for i := 0; i < len(files); i++ {
		c.mapStatus[files[i]] = JobStatus{startTime: -1, state: "pending"}
	}

	// init the status of the nReduce tasks that need to be executed.
	for i := 0; i < nReduce; i++ {
		c.reducerStatus[i] = JobStatus{startTime: -1, state: "pending"}
	}

	c.StartTicker()
	c.server()
	return &c
}
