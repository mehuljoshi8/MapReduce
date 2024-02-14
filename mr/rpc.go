package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.

// Register args/reply

type RegisterArgs struct {
	Pid int
}

type MapJob struct {
	InputFile    string
	MapJobNum    int
	ReducerCount int
}

type ReduceJob struct {
	IntermediateFiles []string
	ReducerNum        int
}

type RequestTaskReply struct {
	MapJob    *MapJob
	ReduceJob *ReduceJob
	Done      bool
}

type ReportMapTaskArgs struct {
	InputFile         string
	IntermediateFiles []string
	Pid               int
}

type ReportReply struct {
	Message string
}

type ReportReduceTaskArgs struct {
	ReducerNum int
	Pid        int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
