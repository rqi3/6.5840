package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "encoding/json"
import "sort"
import "time"

//
// Map functions return a slice of KeyValue.
//
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

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func worker_map(mapf func(string, string) []KeyValue, job_index int, nReduce int, filename string){
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	
	intermediate_kvs := mapf(filename, string(content))
	//separate the key value pairs into buckets
	bucket_kvs := make([][]KeyValue, nReduce)

	for _, kv := range intermediate_kvs{
		bucket_index := ihash(kv.Key) % nReduce
		bucket_kvs[bucket_index] = append(bucket_kvs[bucket_index], kv)
	}

	//write to intermediate files
	intermediate_filenames := make([]string, nReduce)
	for i, kvs := range bucket_kvs{
		temp_file, err := ioutil.TempFile("", "temp")
		if err != nil {
			log.Fatalf("failed to create temp file")
		}
		enc := json.NewEncoder(temp_file)
		for _, kv := range kvs{
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("failed to encode kv")
			}
		}
		temp_file.Close()

		intermediate_filename := fmt.Sprintf("mr-%d-%d", job_index, i)
		os.Rename(temp_file.Name(), intermediate_filename)
		intermediate_filenames[i] = intermediate_filename
	}
	call("Coordinator.CompleteTask", &CompleteTaskArgs{Task_type: "map", Job_index: job_index, Output_filenames: intermediate_filenames}, &CompleteTaskReply{})
}

func worker_reduce(reducef func(string, []string) string, job_index int, filenames []string){
	intermediate_kvs := []KeyValue{}
	for _, filename := range filenames{
		file, err := os.Open(filename)
		if err != nil{
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate_kvs = append(intermediate_kvs, kv)
		}
	}

	sort.Sort(ByKey(intermediate_kvs))

	
	temp_file, err := ioutil.TempFile("", "temp")
	if err != nil {
		log.Fatalf("failed to create temp file")
	}
	i := 0
	for i < len(intermediate_kvs) {
		j := i + 1
		for j < len(intermediate_kvs) && intermediate_kvs[j].Key == intermediate_kvs[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate_kvs[k].Value)
		}
		output := reducef(intermediate_kvs[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(temp_file, "%v %v\n", intermediate_kvs[i].Key, output)
		i = j
	}
	temp_file.Close()
	
	output_filename := fmt.Sprintf("mr-out-%d", job_index)
	os.Rename(temp_file.Name(), output_filename)
	call("Coordinator.CompleteTask", &CompleteTaskArgs{Task_type: "reduce", Job_index: job_index, Output_filenames: []string{output_filename}}, &CompleteTaskReply{})
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	
	wait_time := 100 * time.Millisecond
	for{
		request_args := RequestTaskArgs{}
		request_reply := RequestTaskReply{}
		request_ok := call("Coordinator.RequestTask", &request_args, &request_reply)
		if !request_ok || request_reply.Task_type == "nothing"{
			time.Sleep(wait_time)
			continue
		}

		if request_reply.Task_type != "map" && request_reply.Task_type != "reduce"{
			log.Fatal("Worker: Invalid task type")
		}
		
		if request_reply.Task_type == "map"{
			if len(request_reply.Input_filenames) != 1{
				log.Fatal("Worker: Invalid MAP input filenames")
			}
			worker_map(mapf, request_reply.Job_index, request_reply.NReduce, request_reply.Input_filenames[0])
		} else if request_reply.Task_type == "reduce"{
			worker_reduce(reducef, request_reply.Job_index, request_reply.Input_filenames)
		}
		time.Sleep(wait_time)
		continue
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
