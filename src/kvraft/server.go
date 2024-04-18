package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

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

	last_oper map[int64]int64 //clientId --> last operationId that succeeded

	lastApplied int
	persister *raft.Persister
}

func (kv *KVServer) removeAlertChannel(index int, alertChannelToRemove chan raft.ApplyMsg) {
    existingChannelList := kv.alert_channels[index]
    updatedChannelList := make([]chan raft.ApplyMsg, 0)
    for _, channel := range existingChannelList {
        if channel != alertChannelToRemove {
            updatedChannelList = append(updatedChannelList, channel)
        }
    }
    kv.alert_channels[index] = updatedChannelList
	if len(kv.alert_channels[index]) == 0 {
		delete(kv.alert_channels, index)
	}
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
	apply_msg := <-alert_channel
	kv.mu.Lock()
	kv.removeAlertChannel(index, alert_channel)

	if apply_msg.CommandIndex != index {
		panic("index mismatch")
	}

	if apply_msg.Command.(Op).OperationId != args.OperationId {
		reply.Err = "DifferentThingCommitted"
		return
	}
	value := kv.map_vals[args.Key]
	reply.Value = value

	delete(kv.last_oper, args.ClientId)
	// fmt.Printf("%d: Success! Get %s: %s\n", kv.me, args.Key, value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.last_oper[args.ClientId] == args.OperationId {
		return
	}
	
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
	kv.removeAlertChannel(index, alert_channel)

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

	if kv.last_oper[args.ClientId] == args.OperationId {
		return
	}
	
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
	kv.removeAlertChannel(index, alert_channel)

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
				if kv.last_oper[op.ClientId] != op.OperationId {
					kv.map_vals[op.Key] = op.Value
					kv.last_oper[op.ClientId] = op.OperationId
				}
			} else if op.OpType == "Append" {
				if kv.last_oper[op.ClientId] != op.OperationId {
					kv.map_vals[op.Key] += op.Value
					kv.last_oper[op.ClientId] = op.OperationId
				}
			}
			kv.lastApplied = msg.CommandIndex

			// alert RPCs
			idx := msg.CommandIndex
			if kv.alert_channels[idx] != nil {
				for _, ch := range kv.alert_channels[idx] {
					ch <- msg
				}
			}
		} else {
			if !msg.SnapshotValid {
				panic("SnapshotValid should be true")
			}
			snapshot := msg.Snapshot
			kv.restoreSnapshot(snapshot)
			// kv.persist()
		}
		kv.mu.Unlock()
	}
}

func (kv *KVServer) restoreSnapshot(kvstate []byte) {
	if kvstate == nil || len(kvstate) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(kvstate)
	d := labgob.NewDecoder(r)
	var map_vals map[string]string
	var last_oper map[int64]int64
	var lastApplied int

	if d.Decode(&map_vals) != nil ||
		d.Decode(&last_oper) != nil ||
		d.Decode(&lastApplied) != nil {
		panic("readPersist decode error")
	} else {
		kv.map_vals = map_vals
		kv.last_oper = last_oper
		kv.lastApplied = lastApplied
	}
}



func (kv *KVServer) snapshotLoop() {
	for{
		kv.mu.Lock()
		if kv.maxraftstate != -1 && float32(kv.persister.RaftStateSize()) > float32(kv.maxraftstate) * 0.75 {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.map_vals)
			e.Encode(kv.last_oper)
			e.Encode(kv.lastApplied)
			kvstate := w.Bytes()
			lastApplied := kv.lastApplied
			// kv.mu.Unlock()
			kv.rf.Snapshot(lastApplied, kvstate) //rqi: maybe suspicious we're unlocking here and not before rf.Start
			// kv.mu.Lock()
		}
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
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
	kv.last_oper = make(map[int64]int64)

	// You may need initialization code here.
	kv.persister = persister
	kv.restoreSnapshot(kv.persister.ReadSnapshot())

	go kv.applier()
	go kv.snapshotLoop()

	return kv
}

/*
Test: unreliable net, restarts, partitions, random keys, many clients (4A) ...
panic: test timed out after 10m0s
running tests:
	TestPersistPartitionUnreliableLinearizable4A (5m51s)

goroutine 2333069 [running]:...

I think deadlock ^, possibly caused by waiting for rf.Start() to return, 

Additionally, are you unlocking in your RPC handler while you wait for the command to go through Raft? (https://piazza.com/class/ls556rjxqbi2ie/post/385)

Your solution needs to handle a leader that has called Start() for a Clerk's RPC, but loses its leadership before the request is committed to the log. In this case you should arrange for the Clerk to re-send the request to other servers until it finds the new leader. One way to do this is for the server to detect that it has lost leadership, by noticing that Raft's term has changed or a different request has appeared at the index returned by Start(). If the ex-leader is partitioned by itself, it won't know about new leaders; but any client in the same partition won't be able to talk to a new leader either, so it's OK in this case for the server and client to wait indefinitely until the partition heals.

Could modify where we wait for command to be committed, instead check whether leadership has changed.
- can handle this by calling GetState() every 10ms or so, and seeing if leader/term is the same. If remains the same for a long time, is fine. Then, can remove timeouts from the Client side. Actually, timeouts still needed in the case of dropped messages.

the channels inside of alert_channels need to be removed from the map

2 routines stuck on (just saying [chan receive]):
https://github.com/rqi3/6.5840/blob/916e9a85ac59b152bb2e6e27504215f8c68b27bf/src/kvraft/server.go#L178

5 routines stuck on: kv.mu.Lock()
https://github.com/rqi3/6.5840/blob/916e9a85ac59b152bb2e6e27504215f8c68b27bf/src/kvraft/server.go#L179C3-L179C15

7 routines stuck on: the starting lock of rf.Start().
*/

/*
deal with 4B test later
Test: ops complete fast enough (4B) ...
--- FAIL: TestSpeed4B (17.71s)
    test_test.go:148: duplicate element x 0 918 y in Append result


*/