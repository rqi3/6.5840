package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"

type MapTaskStatus struct{
	time_assigned time.Time
	result []string
}

type ReduceTaskStatus struct{
	time_assigned time.Time
	result string
}

type Coordinator struct {
	coordinator_lock sync.Mutex //remember to acquire
	nReduce int
	input_filenames []string
	map_task_statuses []*MapTaskStatus
	reduce_task_statuses []*ReduceTaskStatus
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.coordinator_lock.Lock()
	defer c.coordinator_lock.Unlock()

	map_tasks_all_done := true
	for _, status := range c.map_task_statuses{
		if status.result == nil{
			map_tasks_all_done = false
			break
		}
	}

	if !map_tasks_all_done{
		//Assign a map task or nothing
		map_task_index := -1
		for i, status := range c.map_task_statuses{
			if status.result == nil && time.Since(status.time_assigned).Seconds() > 10{
				map_task_index = i
				break
			}
		}

		if map_task_index == -1{
			reply.Task_type = "nothing"
			reply.NReduce = c.nReduce
			reply.Job_index = -1
			reply.Input_filenames = nil
		} else {
			c.map_task_statuses[map_task_index].time_assigned = time.Now()
			reply.NReduce = c.nReduce
			reply.Task_type = "map"
			reply.Job_index = map_task_index
			reply.Input_filenames = []string{c.input_filenames[map_task_index]}
		}
	} else {
		//Assign a reduce task or nothing
		reduce_task_index := -1
		for i, status := range c.reduce_task_statuses{
			if status.result == "" && time.Since(status.time_assigned).Seconds() > 10{
				reduce_task_index = i
				break
			}
		}

		if reduce_task_index == -1{
			reply.Task_type = "nothing"
			reply.NReduce = c.nReduce
			reply.Job_index = -1
			reply.Input_filenames = nil
		} else {
			c.reduce_task_statuses[reduce_task_index].time_assigned = time.Now()
			reply.Task_type = "reduce"
			reply.NReduce = c.nReduce
			reply.Job_index = reduce_task_index
			//input filenames are map_task_statuses[i].result[reduce_task_index]
			input_filenames := make([]string, 0)
			for _, status := range c.map_task_statuses{
				input_filenames = append(input_filenames, status.result[reduce_task_index])
			}
			reply.Input_filenames = input_filenames
		}
	}

	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.coordinator_lock.Lock()
	defer c.coordinator_lock.Unlock()

	if args.Task_type != "map" && args.Task_type != "reduce"{
		log.Fatal("CompleteTask: Invalid task type")
	}
	job_index := args.Job_index

	if args.Task_type == "map"{
		c.map_task_statuses[job_index].result = args.Output_filenames
	} else if args.Task_type == "reduce"{
		c.reduce_task_statuses[job_index].result = args.Output_filenames[0]
	}

	return nil
}

//
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
	c.coordinator_lock.Lock()
	defer c.coordinator_lock.Unlock()

	ret := true
	for _, status := range c.reduce_task_statuses{
		if status.result == ""{
			ret = false
			break
		}
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	c.nReduce = nReduce
	c.input_filenames = files
	//initialize map_task_statuses
	c.map_task_statuses = make([]*MapTaskStatus, 0)
	for i := 0; i < len(files); i++{
		c.map_task_statuses = append(c.map_task_statuses, &MapTaskStatus{time.Now().Add(-10 * time.Second), nil})
	}
	//initialize reduce_task_statuses
	c.reduce_task_statuses = make([]*ReduceTaskStatus, 0)
	for i := 0; i < nReduce; i++{
		c.reduce_task_statuses = append(c.reduce_task_statuses, &ReduceTaskStatus{time.Now().Add(-10 * time.Second), ""})
	}

	c.server()
	return &c
}
