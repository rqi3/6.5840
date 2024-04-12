package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Key string
	Value string
	ClientId int64
	OperationId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	map_vals map[string]string

	//for an index, the channels that need to be alerted
	alert_channels map[int][]chan raft.ApplyMsg
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	
	index, _, isLeader := kv.rf.Start(Op{OpType: "Get", Key: args.Key, ClientId: args.ClientId, OperationId: args.OperationId})

	if !isLeader{
		reply.Err = "NotLeader"
		return
	}
	// wait for committed
	alert_channel := make(chan raft.ApplyMsg, 1)

	existing_channel_list := make([]chan raft.ApplyMsg, 0)
	if kv.alert_channels[index] != nil {
		existing_channel_list = kv.alert_channels[index]
	}
	kv.alert_channels[index] = append(existing_channel_list, alert_channel)
	kv.mu.Unlock()
	//if committed, return value
	apply_msg :=<-alert_channel
	kv.mu.Lock()

	if apply_msg.CommandIndex != index {
		panic("index mismatch")
	}

	if apply_msg.Command.(Op).OperationId != args.OperationId {
		reply.Err = "DifferentThingCommitted"
		return
	}
	value := kv.map_vals[args.Key]
	reply.Value = value
	// fmt.Printf("%d: Success! Get %s: %s\n", kv.me, args.Key, value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	
	index, _, isLeader := kv.rf.Start(Op{OpType: "Put", Key: args.Key, Value: args.Value, ClientId: args.ClientId, OperationId: args.OperationId})

	if !isLeader{
		reply.Err = "NotLeader"
		return
	}
	// wait for committed
	alert_channel := make(chan raft.ApplyMsg, 1)

	existing_channel_list := make([]chan raft.ApplyMsg, 0)
	if kv.alert_channels[index] != nil {
		existing_channel_list = kv.alert_channels[index]
	}
	kv.alert_channels[index] = append(existing_channel_list, alert_channel)
	kv.mu.Unlock()
	//if committed, return value
	apply_msg := <-alert_channel
	kv.mu.Lock()

	if apply_msg.CommandIndex != index {
		panic("index mismatch")
	}

	if apply_msg.Command.(Op).OperationId != args.OperationId {
		reply.Err = "DifferentThingCommitted"
		return
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	
	index, _, isLeader := kv.rf.Start(Op{OpType: "Append", Key: args.Key, Value: args.Value, ClientId: args.ClientId, OperationId: args.OperationId})

	if !isLeader{
		reply.Err = "NotLeader"
		return
	}
	// wait for committed
	alert_channel := make(chan raft.ApplyMsg, 1)

	existing_channel_list := make([]chan raft.ApplyMsg, 0)
	if kv.alert_channels[index] != nil {
		existing_channel_list = kv.alert_channels[index]
	}
	kv.alert_channels[index] = append(existing_channel_list, alert_channel)
	kv.mu.Unlock()
	//if committed, return value
	apply_msg := <-alert_channel
	kv.mu.Lock()

	if apply_msg.CommandIndex != index {
		panic("index mismatch")
	}

	if apply_msg.Command.(Op).OperationId != args.OperationId {
		reply.Err = "DifferentThingCommitted"
		return
	}

	// fmt.Printf("%d: Success Append %s %s\n", kv.me, args.Key, args.Value)
}

func (kv *KVServer) applier() {
	for{
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(Op)
			// fmt.Printf("%d: Apply %s %s %s\n", kv.me, op.OpType, op.Key, op.Value)
			if op.OpType == "Get" {
				//do nothing
			} else if op.OpType == "Put" {
				kv.map_vals[op.Key] = op.Value
			} else if op.OpType == "Append" {
				kv.map_vals[op.Key] += op.Value
			}

			// alert RPCs
			idx := msg.CommandIndex
			if kv.alert_channels[idx] != nil {
				for _, ch := range kv.alert_channels[idx] {
					ch <- msg
				}
			}
		}
		kv.mu.Unlock()
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.map_vals = make(map[string]string)
	kv.alert_channels = make(map[int][]chan raft.ApplyMsg)

	// You may need initialization code here.
	go kv.applier()

	return kv
}
