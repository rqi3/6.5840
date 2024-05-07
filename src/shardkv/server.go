package shardkv

import (
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)



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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	map_vals map[string]string

	//for an index, the channels that need to be alerted
	alert_channels map[int][]chan raft.ApplyMsg

	last_oper map[int64]int64 //clientId --> last operationId that succeeded

	lastApplied int

	mck *shardctrler.Clerk
	last_known_config shardctrler.Config
}

func (kv *ShardKV) removeAlertChannel(index int, alertChannelToRemove chan raft.ApplyMsg) {
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

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.last_known_config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = "ErrWrongGroup"
		return
	}

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

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.last_known_config.Shards[key2shard(args.Key)] != kv.gid {
		reply.Err = "ErrWrongGroup"
		return
	}

	if kv.last_oper[args.ClientId] == args.OperationId {
		return
	}
	
	index, _, isLeader := kv.rf.Start(Op{OpType: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, OperationId: args.OperationId})

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
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


func (kv *ShardKV) applier() {
	for{
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(Op)
			// fmt.Printf("%d: Apply %s %s %s\n", kv.me, op.OpType, op.Key, op.Value)
			if op.OpType == "Get" {
				//do nothing
				// rqi 4/25: maybe should move delete(kv.last_oper) to here? probably doens't matter
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
			// if !msg.SnapshotValid {
			// 	panic("SnapshotValid should be true")
			// }
			// snapshot := msg.Snapshot
			// kv.restoreSnapshot(snapshot)
			// kv.persist()
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) updateConfig(){
	for{
		config := kv.mck.Query(-1)
		kv.mu.Lock()
		kv.last_known_config = config
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.map_vals = make(map[string]string)
	kv.alert_channels = make(map[int][]chan raft.ApplyMsg)
	kv.last_oper = make(map[int64]int64)

	// You may need initialization code here.
	// kv.persister = persister
	// kv.restoreSnapshot(kv.persister.ReadSnapshot())

	go kv.applier()
	// go kv.snapshotLoop()
	go kv.updateConfig()

	return kv
}
