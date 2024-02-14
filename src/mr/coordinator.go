package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type MapTaskStatus{
	time_assigned time.Time
	result []string
}

type ReduceTaskStatus{
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

	map_tasks_all_done = true
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
			reply.task_type = "nothing"
			reply.nReduce = c.nReduce
			reply.job_index = -1
			reply.input_filenames = nil
		}
		else{
			status.time_assigned = time.Now()
			reply.nReduce = c.nReduce
			reply.task_type = "map"
			reply.job_index = map_task_index
			reply.input_filenames = []string{c.input_filenames[i]}
		}
	}
	else{
		//Assign a reduce task or nothing
		reduce_task_index := -1
		for i, status := range c.reduce_task_statuses{
			if status.result == "" && time.Since(status.time_assigned).Seconds() > 10{
				reduce_task_index = i
				break
			}
		}

		if reduce_task_index == -1{
			reply.task_type = "nothing"
			reply.nReduce = c.nReduce
			reply.job_index = -1
			reply.input_filenames = nil
		}
		else{
			status.time_assigned = time.Now()
			reply.task_type = "reduce"
			reply.nReduce = c.nReduce
			reply.job_index = reduce_task_index
			//input filenames are map_task_statuses[i].result[reduce_task_index]
			input_filenames = make([]string, 0)
			for i, status := range c.map_task_statuses{
				input_filenames = append(input_filenames, status.result[reduce_task_index])
			}
			reply.input_filenames = input_filenames
		}
	}

	return nil
}

func (c *Coordinator) CompleteTask(args *CompleteTaskArgs, reply *CompleteTaskReply) error {
	c.coordinator_lock.Lock()
	defer c.coordinator_lock.Unlock()

	if args.task_type != "map" && args.task_type != "reduce"{
		log.Fatal("CompleteTask: Invalid task type")
	}
	job_index := args.job_index

	if args.task_type == "map"{
		c.map_task_statuses[job_index].result = args.output_filenames
	}
	else if args.task_type == "reduce"{
		c.reduce_task_statuses[job_index].result = args.output_filenames[0]
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
