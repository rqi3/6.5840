package kvsrv

import (
	"log"
	"strconv"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func strToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	return i
}

type OperDescription struct {
	OperationId int64
	Result string
}
type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	map_vals map[string]string

	//the last operation that was performed for this client ID
	last_oper map[int64]OperDescription
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.last_oper, strToInt(args.ClientId))
	reply.Value = kv.map_vals[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.last_oper[strToInt(args.ClientId)].OperationId == strToInt(args.OperationId) {
		return
	}

	kv.map_vals[args.Key] = args.Value

	kv.last_oper[strToInt(args.ClientId)] = OperDescription{strToInt(args.OperationId), ""}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.last_oper[strToInt(args.ClientId)].OperationId == strToInt(args.OperationId) {
		reply.Value = kv.last_oper[strToInt(args.ClientId)].Result
		return
	}

	previous_value := kv.map_vals[args.Key]
	kv.map_vals[args.Key] = previous_value + args.Value
	reply.Value = previous_value

	kv.last_oper[strToInt(args.ClientId)] = OperDescription{strToInt(args.OperationId), reply.Value}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.map_vals = make(map[string]string)
	kv.last_oper = make(map[int64]OperDescription)

	return kv
}
