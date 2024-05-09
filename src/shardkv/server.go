package shardkv

import (
	"fmt"
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
	NewConfig shardctrler.Config

	ShardId int
	NewConfigNum int
	ShardData map[string]string
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

	serving_shards map[int]struct{} //which shards is this server serving

	//currently going from old_config to new_config, need to import and export these remaining shards
	old_config shardctrler.Config
	new_config shardctrler.Config
	shards_to_import map[int]int
	shards_to_export map[int]int
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

func (kv *ShardKV) addAlertChannel(index int, alertChannelToAdd chan raft.ApplyMsg) {
	existingChannelList := kv.alert_channels[index]
	updatedChannelList := append(existingChannelList, alertChannelToAdd)
	kv.alert_channels[index] = updatedChannelList
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.serving_shards[key2shard(args.Key)]; !ok {
		// fmt.Printf("%d-%d: Not serving %v\n", kv.gid, kv.me, kv.serving_shards)
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

	kv.addAlertChannel(index, alert_channel)
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

	if kv.new_config.Shards[key2shard(args.Key)] != kv.gid {
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

	kv.addAlertChannel(index, alert_channel)
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

type ExportShardArgs struct{
	ShardId int
	NewConfigNum int
	ShardData map[string]string
}

type ExportShardReply struct {
	Err string
}

func copyShardData(shard_data map[string]string) map[string]string {
	new_shard_data := make(map[string]string)
	for key, val := range shard_data {
		new_shard_data[key] = val
	}
	return new_shard_data
}

func configCopy(config shardctrler.Config) shardctrler.Config {
	result := shardctrler.Config{
		Num:    config.Num,
		Shards: config.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range config.Groups {
		result.Groups[gid] = make([]string, 0)
		result.Groups[gid] = append(result.Groups[gid], servers...) 
	}
	return result
}

func (kv *ShardKV) ExportShardRPC(args *ExportShardArgs, reply *ExportShardReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// fmt.Printf("%d-%d: ExportShardRPC\n", kv.gid, kv.me)

	if args.NewConfigNum < kv.new_config.Num {
		reply.Err = "Success" //already applied this shard import!
		return
	}

	if args.NewConfigNum > kv.new_config.Num {
		reply.Err = "ConfigFuture"
		return
	}

	_, ok := kv.shards_to_import[args.ShardId]
	if !ok {
		reply.Err = "Success" //already imported this shard
		return
	}


	operation_id := nrand()
	shard_data := copyShardData(args.ShardData)
	index, _, isLeader := kv.rf.Start(Op{OpType: "ShardImport", OperationId: operation_id, ShardId: args.ShardId, NewConfigNum: args.NewConfigNum, ShardData: shard_data})
	if !isLeader {
		reply.Err = "NotLeader"
		return
	}
	// wait for committed
	alert_channel := make(chan raft.ApplyMsg, 1)

	kv.addAlertChannel(index, alert_channel)
	kv.mu.Unlock()
	//if committed, return value
	apply_msg := <-alert_channel
	kv.mu.Lock()
	kv.removeAlertChannel(index, alert_channel)

	if apply_msg.CommandIndex != index {
		panic("index mismatch")
	}

	if apply_msg.Command.(Op).OperationId != operation_id {
		reply.Err = "DifferentThingCommitted"
		return
	}
	reply.Err = "Success"
}

func (kv *ShardKV) exportShard(new_config_num int, shard_id int) {
	// fmt.Printf("%d-%d: exporting shard %d\n", kv.gid, kv.me, shard_id)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard_data := make(map[string]string)
	for key, val := range kv.map_vals {
		if key2shard(key) == shard_id {
			shard_data[key] = val
		}
	}

	if servers, ok := kv.new_config.Groups[kv.shards_to_export[shard_id]]; ok {
		success_channel := make(chan bool, len(servers))
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			args := ExportShardArgs{ShardId: shard_id, NewConfigNum: new_config_num, ShardData: shard_data}
			go func() {
				for{
					reply := ExportShardReply{}
					ok := srv.Call("ShardKV.ExportShardRPC", &args, &reply)
					if ok && reply.Err == "Success" {
						success_channel <- true
						return
					}
				}
			}()
		}
		kv.mu.Unlock()
		<- success_channel
		kv.mu.Lock()
		delete(kv.shards_to_export, shard_id)
	} else {
		panic("Weid, group does not exist?")
	}
}

func (kv *ShardKV) applier() {
	for{
		msg := <-kv.applyCh
		kv.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(Op)
			// fmt.Printf("%d-%d: Apply %s %s %s\n", kv.gid, kv.me, op.OpType, op.Key, op.Value)
			if op.OpType == "Get" || op.OpType == "Put" || op.OpType == "Append" {
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
				
			} else if op.OpType == "Seal" {
				// fmt.Printf("%d-%d: op.NewConfig: %+v\n", kv.gid, kv.me, op.NewConfig)
				if op.NewConfig.Num <= kv.new_config.Num {
					//already processed
				} else {
					if op.NewConfig.Num != kv.new_config.Num+1 {
						// fmt.Printf("%d %d: op.NewConfig.Num = %d, kv.new_config.Num = %d\n", kv.gid, kv.me, op.NewConfig.Num, kv.new_config.Num)
						panic("op.NewConfig.Num != kv.new_config.Num+1")
					}
					// wait for shard imports and exports to finish
					waiting := 0
					for len(kv.shards_to_export) > 0 || len(kv.shards_to_import) > 0 {
						time.Sleep(10 * time.Millisecond)
						waiting += 1
						if waiting > 1000 {
							fmt.Printf("%d-%d: waiting > 1000, shards_to_export = %v, shards_to_import = %v\n", kv.gid, kv.me, kv.shards_to_export, kv.shards_to_import)
							panic("waiting > 1000") // maybe can remove this, but helpful for debug
						}
					}

					kv.old_config = configCopy(kv.new_config)
					kv.new_config = configCopy(op.NewConfig)
					// fmt.Printf("%d %d: kv.old_config: %+v\n", kv.gid, kv.me, kv.old_config)
					// fmt.Printf("%d %d: kv.new_config: %+v\n", kv.gid, kv.me, kv.new_config)

					//figure out which shards to import, export
					for i := 0; i < shardctrler.NShards; i++ {
						old_gid := kv.old_config.Shards[i]
						new_gid := kv.new_config.Shards[i]
						if new_gid == kv.gid && old_gid != kv.gid{
							//import shard
							if old_gid == 0 {
								//starting to serve shard
								kv.serving_shards[i] = struct{}{}
							} else {
								kv.shards_to_import[i] = old_gid
							}
							
						} else if new_gid != kv.gid && old_gid == kv.gid {
							//export shard
							// fmt.Printf("Exporting shard %d\n", i)
							kv.shards_to_export[i] = new_gid
							delete(kv.serving_shards, i)
						}
					} 

					// start goroutines to export shards
					for shard_id := range kv.shards_to_export {
						go kv.exportShard(kv.new_config.Num, shard_id)
					}
				}
			} else if op.OpType == "ShardImport" {
				// receiving a shard import
				if op.NewConfigNum != kv.new_config.Num {	
					panic("Bad config num!")
				}

				for key, value := range op.ShardData {
					kv.map_vals[key] = value
				}
				
				_, ok := kv.shards_to_import[op.ShardId]
				if ok {
					delete(kv.shards_to_import, op.ShardId)
				}
				kv.serving_shards[op.ShardId] = struct{}{}
			}
		} else {
			// if !msg.SnapshotValid {
			// 	panic("SnapshotValid should be true")
			// }
			// snapshot := msg.Snapshot
			// kv.restoreSnapshot(snapshot)
			// kv.persist()
		}

		// alert RPCs
		idx := msg.CommandIndex
		if kv.alert_channels[idx] != nil {
			for _, ch := range kv.alert_channels[idx] {
				ch <- msg
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) updateConfig(){
	for{
		kv.mu.Lock()
		// fmt.Printf("%d-%d: querying %d\n", kv.gid, kv.me, kv.new_config.Num+1)
		// fmt.Printf("%d-%d: new_config: %v\n", kv.gid, kv.me, kv.new_config)
		config := kv.mck.Query(kv.new_config.Num+1) //is this safe? query could take a long time. But otherwise data race on kv.mck?

		if config.Num == kv.new_config.Num+1 {
			operation_id := nrand()
			index, _, isLeader := kv.rf.Start(Op{OpType: "Seal", OperationId: operation_id, NewConfig: configCopy(config)})
			// fmt.Printf("%d-%d: updateConfig, config: %v\n", kv.gid, kv.me, config)
			if isLeader {
				alert_channel := make(chan raft.ApplyMsg, 1)
				kv.addAlertChannel(index, alert_channel)
				kv.mu.Unlock()

				apply_msg := <-alert_channel

				kv.mu.Lock()
				kv.removeAlertChannel(index, alert_channel)

				if apply_msg.CommandIndex != index {
					panic("index mismatch")
				}
			}
			
		}
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

	kv.shards_to_import = make(map[int]int)
	kv.shards_to_export = make(map[int]int)
	kv.old_config = shardctrler.Config{}
	kv.new_config = shardctrler.Config{}
	kv.serving_shards = make(map[int]struct{})

	// You may need initialization code here.
	// kv.persister = persister
	// kv.restoreSnapshot(kv.persister.ReadSnapshot())

	go kv.applier()
	// go kv.snapshotLoop()
	go kv.updateConfig()

	return kv
}
