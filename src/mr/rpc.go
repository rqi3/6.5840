package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type RequestTaskArgs struct {

}

type RequestTaskReply struct {
	Task_type string //either "map", "reduce", "nothing"
	Job_index int
	NReduce int
	Input_filenames []string
}

type CompleteTaskArgs struct {
	Task_type string
	Job_index int
	Output_filenames []string
}

type CompleteTaskReply struct {
	
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
