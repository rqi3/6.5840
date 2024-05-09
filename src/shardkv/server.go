package shardkv

import (
	"bytes"
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
	ShardData ShardInfo
}

type ShardInfo struct {
	ShardId int
	KeyVals map[string]string
	LastOper map[int64]int64
}

type MyApplyMsg struct {
	raft_apply_msg raft.ApplyMsg
	Err string
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
	shard_infos [shardctrler.NShards]ShardInfo

	//for an index, the channels that need to be alerted
	alert_channels map[int][]chan MyApplyMsg

	

	lastApplied int

	mck *shardctrler.Clerk

	serving_shards map[int]struct{} //which shards is this server serving

	//currently going from old_config to new_config, need to import and export these remaining shards
	old_config shardctrler.Config
	new_config shardctrler.Config
	shards_to_import map[int]int
	shards_to_export map[int]int
	all_shards_to_export map[int]int //all shards that needed to be exported in this movement phase

	persister *raft.Persister
}

func (kv *ShardKV) removeAlertChannel(index int, alertChannelToRemove chan MyApplyMsg) {
    existingChannelList := kv.alert_channels[index]
    updatedChannelList := make([]chan MyApplyMsg, 0)
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

func (kv *ShardKV) addAlertChannel(index int, alertChannelToAdd chan MyApplyMsg) {
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
	alert_channel := make(chan MyApplyMsg, 1)

	kv.addAlertChannel(index, alert_channel)
	kv.mu.Unlock()
	//if committed, return value
	apply_msg := <-alert_channel
	kv.mu.Lock()
	kv.removeAlertChannel(index, alert_channel)

	if apply_msg.raft_apply_msg.CommandIndex != index {
		panic("index mismatch")
	}
	
	if apply_msg.raft_apply_msg.Command.(Op).OperationId != args.OperationId {
		reply.Err = "DifferentThingCommitted"
		return
	}

	if apply_msg.Err == "ErrWrongGroup" {
		reply.Err = "ErrWrongGroup"
		return
	}
	if apply_msg.Err != ""{
		panic("Unknown error")
	}
	value := kv.shard_infos[key2shard(args.Key)].KeyVals[args.Key]
	reply.Value = value

	delete(kv.shard_infos[key2shard(args.Key)].LastOper, args.ClientId)
	// fmt.Printf("%d: Success! Get %s: %s\n", kv.me, args.Key, value)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.serving_shards[key2shard(args.Key)]; !ok {
		// fmt.Printf("%d-%d: Not serving %v\n", kv.gid, kv.me, kv.serving_shards)
		reply.Err = "ErrWrongGroup"
		return
	}

	if kv.shard_infos[key2shard(args.Key)].LastOper[args.ClientId] == args.OperationId {
		return
	}
	
	index, _, isLeader := kv.rf.Start(Op{OpType: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, OperationId: args.OperationId})

	if !isLeader{
		reply.Err = "NotLeader"
		return
	}
	// wait for committed
	alert_channel := make(chan MyApplyMsg, 1)

	kv.addAlertChannel(index, alert_channel)
	kv.mu.Unlock()
	//if committed, return value
	apply_msg := <-alert_channel
	kv.mu.Lock()
	kv.removeAlertChannel(index, alert_channel)

	if apply_msg.raft_apply_msg.CommandIndex != index {
		panic("index mismatch")
	}

	if apply_msg.raft_apply_msg.Command.(Op).OperationId != args.OperationId {
		reply.Err = "DifferentThingCommitted"
		return
	}
	if apply_msg.Err == "ErrWrongGroup" {
		reply.Err = "ErrWrongGroup"
		return
	}
	if apply_msg.Err != ""{
		panic("Unknown error")
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
	ShardData ShardInfo
	RequesterGroup int
	RequesterId int
}

type ExportShardReply struct {
	Err string
}

func copyShardData(shard_data ShardInfo) ShardInfo {
	new_shard_data := ShardInfo{}
	new_shard_data.ShardId = shard_data.ShardId
	new_shard_data.KeyVals = make(map[string]string)
	new_shard_data.LastOper = make(map[int64]int64)

	for key, val := range shard_data.KeyVals {
		new_shard_data.KeyVals[key] = val
	}
	for key, val := range shard_data.LastOper {
		new_shard_data.LastOper[key] = val
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
	// fmt.Printf("%d-%d: ExportShardRPC %v\n", kv.gid, kv.me, args)

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

	// fmt.Printf("%d-%d: ExportShardRPC %d waiting to be committed. From %d-%d\n", kv.gid, kv.me, args.ShardId, args.RequesterGroup, args.RequesterId)

	// wait for committed
	alert_channel := make(chan MyApplyMsg, 1)

	kv.addAlertChannel(index, alert_channel)
	kv.mu.Unlock()
	//if committed, return value
	apply_msg := <-alert_channel
	kv.mu.Lock()
	kv.removeAlertChannel(index, alert_channel)

	if apply_msg.raft_apply_msg.CommandIndex != index {
		panic("index mismatch")
	}

	if apply_msg.raft_apply_msg.Command.(Op).OperationId != operation_id {
		reply.Err = "DifferentThingCommitted"
		return
	}
	reply.Err = "Success"
	// fmt.Printf("%d-%d: ExportShardRPC %d successful!!! from %d-%d\n", kv.gid, kv.me, args.ShardId, kv.gid, kv.me)
}

func (kv *ShardKV) exportShard(new_config_num int, shard_id int) {
	kv.mu.Lock()
	// fmt.Printf("%d-%d: exporting shard %d\n", kv.gid, kv.me, shard_id)
	defer kv.mu.Unlock()
	shard_data := copyShardData(kv.shard_infos[shard_id])

	if servers, ok := kv.new_config.Groups[kv.shards_to_export[shard_id]]; ok {
		success_channel := make(chan bool, len(servers))
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			args := ExportShardArgs{ShardId: shard_id, NewConfigNum: new_config_num, ShardData: copyShardData(shard_data), RequesterGroup: kv.gid, RequesterId: kv.me}
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
		// fmt.Printf("%d-%d: exported shard %d %v\n", kv.gid, kv.me, shard_id, kv.shards_to_export)
	} else {
		if new_config_num != kv.new_config.Num {
			//config num has been changed in the meantime
		} else {
			panic("Weid, group does not exist?")
		}
		
	}
}

func (kv *ShardKV) applier() {
	for{
		msg := <-kv.applyCh
		kv.mu.Lock()
		err := ""
		if msg.CommandValid {
			op := msg.Command.(Op)
			// fmt.Printf("%d-%d: Apply %s %s %s\n", kv.gid, kv.me, op.OpType, op.Key, op.Value)
			
			if op.OpType == "Get" || op.OpType == "Put" || op.OpType == "Append" {
				_, ok := kv.serving_shards[key2shard(op.Key)]
				if !ok {
					err = "ErrWrongGroup"
				} else {
					if op.OpType == "Get" {
						//do nothing
						// rqi 4/25: maybe should move delete(kv.last_oper) to here? probably doens't matter
					} else if op.OpType == "Put" {
						if kv.shard_infos[key2shard(op.Key)].LastOper[op.ClientId] != op.OperationId {
							kv.shard_infos[key2shard(op.Key)].KeyVals[op.Key] = op.Value
							kv.shard_infos[key2shard(op.Key)].LastOper[op.ClientId] = op.OperationId
						}
					} else if op.OpType == "Append" {
						if kv.shard_infos[key2shard(op.Key)].LastOper[op.ClientId] != op.OperationId {
							kv.shard_infos[key2shard(op.Key)].KeyVals[op.Key] += op.Value
							kv.shard_infos[key2shard(op.Key)].LastOper[op.ClientId] = op.OperationId
						}
					}
				}
				
				
				
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
						kv.mu.Unlock()
						time.Sleep(10 * time.Millisecond)
						kv.mu.Lock()
						waiting += 1
						if waiting > 1000 {
							// fmt.Printf("%d-%d: waiting > 1000, shards_to_export = %v, shards_to_import = %v\n", kv.gid, kv.me, kv.shards_to_export, kv.shards_to_import)
							panic("waiting > 1000") // maybe can remove this, but helpful for debug
						}
					}

					kv.old_config = configCopy(kv.new_config)
					kv.new_config = configCopy(op.NewConfig)
					// fmt.Printf("%d-%d: kv.old_config: %+v\n", kv.gid, kv.me, kv.old_config)
					// fmt.Printf("%d-%d: kv.new_config: %+v\n", kv.gid, kv.me, kv.new_config)

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
							// fmt.Printf("%d-%d:Exporting shard %d\n", kv.gid, kv.me, i)
							kv.shards_to_export[i] = new_gid
							delete(kv.serving_shards, i)
						}
					}

					kv.all_shards_to_export = make(map[int] int)
					for shard_id, gid := range kv.shards_to_export {
						kv.all_shards_to_export[shard_id] = gid
					}

					// fmt.Printf("%d-%d: kv.shards_to_export: %+v kv.shards_to_import: %+v\n", kv.gid, kv.me, kv.shards_to_export, kv.shards_to_import)

					// start goroutines to export shards
					for shard_id := range kv.shards_to_export {
						go kv.exportShard(kv.new_config.Num, shard_id)
					}
				}
			} else if op.OpType == "ShardImport" {
				// receiving a shard import
				if op.NewConfigNum > kv.new_config.Num {	
					// fmt.Printf("%d-%d: op.NewConfigNum = %d, kv.new_config.Num = %d\n", kv.gid, kv.me, op.NewConfigNum, kv.new_config.Num)
					panic("Bad config num!")
				}

				if op.NewConfigNum == kv.new_config.Num {
					// already processed
					kv.shard_infos[op.ShardId] = copyShardData(op.ShardData)
					
					_, ok := kv.shards_to_import[op.ShardId]
					if ok {
						delete(kv.shards_to_import, op.ShardId)
					}
					kv.serving_shards[op.ShardId] = struct{}{}
				}
				
			}
			kv.lastApplied = msg.CommandIndex
			// fmt.Printf("%d-%d: kv.lastApplied: %d\n", kv.gid, kv.me, kv.lastApplied)
		} else {
			if !msg.SnapshotValid {
				panic("SnapshotValid should be true")
			}
			snapshot := msg.Snapshot
			kv.restoreSnapshot(snapshot)
		}

		// alert RPCs
		idx := msg.CommandIndex
		if kv.alert_channels[idx] != nil {
			for _, ch := range kv.alert_channels[idx] {
				ch <- MyApplyMsg{raft_apply_msg: msg, Err: err} // new_config is updated by msg
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

		if config.Num == kv.new_config.Num+1 && len(kv.shards_to_import) == 0{
			operation_id := nrand()
			index, _, isLeader := kv.rf.Start(Op{OpType: "Seal", OperationId: operation_id, NewConfig: configCopy(config)})
			// fmt.Printf("%d-%d: updateConfig, config: %v\n", kv.gid, kv.me, config)
			if isLeader {
				alert_channel := make(chan MyApplyMsg, 1)
				kv.addAlertChannel(index, alert_channel)
				kv.mu.Unlock()

				apply_msg := <-alert_channel

				kv.mu.Lock()
				kv.removeAlertChannel(index, alert_channel)

				if apply_msg.raft_apply_msg.CommandIndex != index {
					panic("index mismatch")
				}
			}
			
		}
		kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) restoreSnapshot(kvstate []byte) {
	if kvstate == nil || len(kvstate) < 1 { // bootstrap without any state?
		return
	}
	
	r := bytes.NewBuffer(kvstate)
	d := labgob.NewDecoder(r)
	var shard_infos [shardctrler.NShards]ShardInfo
	var serving_shards map[int]struct{}
	var old_config shardctrler.Config
	var new_config shardctrler.Config
	var shards_to_import map[int]int
	var shards_to_export map[int]int
	var all_shards_to_export map[int]int
	var lastApplied int
	if d.Decode(&shard_infos) != nil ||
		d.Decode(&serving_shards) != nil ||
		d.Decode(&old_config) != nil ||
		d.Decode(&new_config) != nil ||
		d.Decode(&shards_to_import) != nil ||
		d.Decode(&shards_to_export) != nil ||
		d.Decode(&all_shards_to_export) != nil ||
		d.Decode(&lastApplied) != nil {
		panic("readPersist decode error")
	} else {
		kv.shard_infos = shard_infos
		kv.serving_shards = serving_shards
		kv.old_config = old_config
		kv.new_config = new_config
		kv.shards_to_import = shards_to_import
		kv.shards_to_export = shards_to_export
		kv.all_shards_to_export = all_shards_to_export
		for shard_id, gid := range kv.all_shards_to_export { //all shards should be exported again
			kv.shards_to_export[shard_id] = gid
		}
		kv.lastApplied = lastApplied
	}

	// fmt.Printf("%d-%d: restoreSnapshot %v\n", kv.gid, kv.me, kv.new_config)
	// fmt.Printf("%d-%d: kv.lastApplied: %d\n", kv.gid, kv.me, kv.lastApplied)
	// fmt.Printf("%d-%d: kv.shards_to_export: %v\n", kv.gid, kv.me, kv.shards_to_export)

	for shard_id := range kv.shards_to_export {
		// fmt.Printf("%d-%d: starting restoreSnapshot exportShard %d\n", kv.gid, kv.me, shard_id)
		go kv.exportShard(kv.new_config.Num, shard_id)
	}
}

func (kv *ShardKV) snapshotLoop() {
	for{
		kv.mu.Lock()
		if kv.maxraftstate != -1 && float32(kv.persister.RaftStateSize()) > float32(kv.maxraftstate) * 0.75 {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.shard_infos)
			e.Encode(kv.serving_shards)
			e.Encode(kv.old_config)
			e.Encode(kv.new_config)
			e.Encode(kv.shards_to_import)
			e.Encode(kv.shards_to_export)
			e.Encode(kv.all_shards_to_export)
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
	for i := 0; i < shardctrler.NShards; i++ {
		kv.shard_infos[i] = ShardInfo{
			ShardId: i,
			KeyVals: make(map[string]string),
			LastOper: make(map[int64]int64),
		}
	}
	kv.alert_channels = make(map[int][]chan MyApplyMsg)
	

	kv.shards_to_import = make(map[int]int)
	kv.shards_to_export = make(map[int]int)
	kv.old_config = shardctrler.Config{}
	kv.new_config = shardctrler.Config{}
	kv.serving_shards = make(map[int]struct{})
	kv.all_shards_to_export = make(map[int]int)

	// You may need initialization code here.
	kv.mu.Lock()
	kv.persister = persister
	// fmt.Printf("%d-%d: before restoreSnapshot\n", kv.gid, kv.me)
	kv.restoreSnapshot(kv.persister.ReadSnapshot())

	// fmt.Printf("%d-%d: after restoreSnapshot\n", kv.gid, kv.me)

	go kv.applier()
	// fmt.Printf("%d-%d: after restoreSnapshot 2\n", kv.gid, kv.me)
	// if kv.maxraftstate != -1 && kv.maxraftstate == 1000{
	go kv.snapshotLoop()
	// }
	// fmt.Printf("%d-%d: after restoreSnapshot 3\n", kv.gid, kv.me)
	// fmt.Printf("%d-%d: starting restoreSnapshot exportShard LOOP\n", kv.gid, kv.me)
	
	go kv.updateConfig()
	kv.mu.Unlock()

	return kv
}
