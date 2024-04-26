package shardctrler

import (
	"fmt"
	"strconv"
	"sync"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	//for an index, the channels that need to be alerted
	alert_channels map[int][]chan raft.ApplyMsg

	last_oper map[int64]int64 //clientId --> last operationId that succeeded

	lastApplied int
}


type Op struct {
	// Your data here.
	OpType string
	ConfigsIndex int
	NewConfig Config
	ClientId int64
	OperationId int64
	Num int // for query
}

func configCopy(config Config) Config {
	result := Config{
		Num:    config.Num,
		Shards: config.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range config.Groups {
		result.Groups[gid] = make([]string, 0)
		for _, server := range servers {
			result.Groups[gid] = append(result.Groups[gid], server)
		}
	}
	return result
}

func (sc *ShardCtrler) removeAlertChannel(index int, alertChannelToRemove chan raft.ApplyMsg) {
    existingChannelList := sc.alert_channels[index]
    updatedChannelList := make([]chan raft.ApplyMsg, 0)
    for _, channel := range existingChannelList {
        if channel != alertChannelToRemove {
            updatedChannelList = append(updatedChannelList, channel)
        }
    }
    sc.alert_channels[index] = updatedChannelList
	if len(sc.alert_channels[index]) == 0 {
		delete(sc.alert_channels, index)
	}
}

func (sc *ShardCtrler) attemptModifyConfig(new_config Config, client_id int64, operation_id int64) string {
	index, _, isLeader := sc.rf.Start(Op{
		OpType: "ConfigChange",
		ConfigsIndex: len(sc.configs), 
		NewConfig: new_config,
		ClientId: client_id, 
		OperationId: operation_id,
	})
	
	if !isLeader{
		return "NotLeader"
	}

	alert_channel := make(chan raft.ApplyMsg, 1)
	existing_channel_list := make([]chan raft.ApplyMsg, 0)
	if sc.alert_channels[index] != nil {
		existing_channel_list = sc.alert_channels[index]
	}
	sc.alert_channels[index] = append(existing_channel_list, alert_channel)
	sc.mu.Unlock()
	apply_msg := <-alert_channel
	sc.mu.Lock()
	sc.removeAlertChannel(index, alert_channel)

	if apply_msg.CommandIndex != index {
		panic("index mismatch")
	}

	if apply_msg.Command.(Op).OperationId != operation_id {
		return "DifferentThingCommitted"
	}

	return "Success"
}

func rebalanceShards(config Config) [NShards]int {
	new_shards := [NShards]int{}
	for i := 0; i < NShards; i++ {
		new_shards[i] = config.Shards[i]
	}

	if len(config.Groups) == 0 {
		return new_shards
	}

	getSmallGroup := func (shard_assignment [NShards]int) []int{
		shard_count := make(map[int]int)
		for gid := range config.Groups {
			shard_count[gid] = 0
		}
		for i := 0; i < NShards; i++ {
			if shard_assignment[i] == 0 {
				continue
			}
			shard_count[shard_assignment[i]]++
		}
		small_group_cnt := -1
		small_group_idx := -1
		for gid, count := range shard_count {
			if small_group_idx == -1 || count < small_group_cnt || (count == small_group_cnt && gid < small_group_idx) {
				small_group_idx = gid
				small_group_cnt = count
			}
		}

		shard_idx := 0
		for i := 0; i < NShards; i++ {
			if shard_assignment[i] == small_group_idx {
				shard_idx = i
				break
			}
		}
		return []int{small_group_idx, small_group_cnt, shard_idx}
	}
	getLargeGroup := func (shard_assignment [NShards]int) []int{
		shard_count := make(map[int]int)
		for gid := range config.Groups {
			shard_count[gid] = 0
		}
		for i := 0; i < NShards; i++ {
			if config.Shards[i] == 0 {
				continue
			}
			shard_count[shard_assignment[i]]++
		}
		large_group_cnt := -1
		large_group_idx := -1
		for gid, count := range shard_count {
			if large_group_idx == -1 || count > large_group_cnt || (count == large_group_cnt && gid > large_group_idx){
				large_group_idx = gid
				large_group_cnt = count
			}
		}

		shard_idx := 0
		for i := 0; i < NShards; i++ {
			if shard_assignment[i] == large_group_idx {
				shard_idx = i
				break
			}
		}
		return []int{large_group_idx, large_group_cnt, shard_idx}
	}

	//first, reassign shards pointing to 0
	for i := 0; i < NShards; i++ {
		if new_shards[i] == 0 {
			// set new_shards[i] to GID with smallest number of shards
			small_group_arr := getSmallGroup(new_shards)
			new_shards[i] = small_group_arr[0]
		}
	}

	for i := 0; i < NShards; i++ {
		if new_shards[i] == 0 {
			panic("new_shards[i] == 0")
		}
	}

	//now, rebalance shards
	for {
		small_group_arr := getSmallGroup(new_shards)
		large_group_arr := getLargeGroup(new_shards)
		if large_group_arr[1] >= small_group_arr[1] + 2 {
			new_shards[large_group_arr[2]] = small_group_arr[0]
		} else {
			break
		}
	}

	return new_shards
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.last_oper[args.ClientId] == args.OperationId {
		return
	}

	new_config := configCopy(sc.configs[len(sc.configs) - 1])
	//go through new shards in increasing order, assign to lowest num shards 
	for gid := range args.Servers {
		if _, ok := new_config.Groups[gid]; ok {
			panic("Gid already exists in config!")
		}
		new_config.Groups[gid] = args.Servers[gid]
	}
	new_config.Shards = rebalanceShards(new_config) //old config, extra shard indices
	status := sc.attemptModifyConfig(new_config, args.ClientId, args.OperationId)
	if status == "Success" {
		return
	} 

	if status == "NotLeader" {
		reply.WrongLeader = true
		reply.Err = "Test"
		reply.Err = Err(status)
		return
	}

	if status != "DifferentThingCommitted" {
		panic("Unexpected status: " + status)
	}

	reply.Err = Err(status)
	return
}

func contains(s []int, x int) bool {
	for _, y := range s {
		if y == x {
			return true
		}
	}

	return false
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.last_oper[args.ClientId] == args.OperationId {
		return
	}

	new_config := configCopy(sc.configs[len(sc.configs) - 1])
	//go through new shards in increasing order, assign to lowest num shards
	
	for i := 0; i < NShards; i++ {
		if contains(args.GIDs, new_config.Shards[i]){
			new_config.Shards[i] = 0
		}
	}
	for _, gid := range args.GIDs {
		delete(new_config.Groups, gid)
	}

	new_config.Shards = rebalanceShards(new_config) //old config, extra shard indices
	
	status := sc.attemptModifyConfig(new_config, args.ClientId, args.OperationId)
	if status == "Success" {
		return
	} 

	if status == "NotLeader" {
		reply.WrongLeader = true
		reply.Err = Err(status)
		return
	}

	if status != "DifferentThingCommitted" {
		panic("Unexpected status: " + status)
	}

	reply.Err = Err(status)
	return
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if sc.last_oper[args.ClientId] == args.OperationId {
		return
	}

	new_config := configCopy(sc.configs[len(sc.configs) - 1])
	//go through new shards in increasing order, assign to lowest num shards
	new_config.Shards[args.Shard] = args.GID
	status := sc.attemptModifyConfig(new_config, args.ClientId, args.OperationId)
	if status == "Success" {
		return
	} 

	if status == "NotLeader" {
		reply.WrongLeader = true
		reply.Err = Err(status)
		return
	}

	if status != "DifferentThingCommitted" {
		panic("Unexpected status: " + status)
	}

	reply.Err = Err(status)
	return
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// fmt.Printf("%d: Query args: %v\n", sc.me, args)
	// Your code here.
	sc.mu.Lock()
	defer sc.mu.Unlock()

	actual_args_num := args.Num
	if actual_args_num == -1 || actual_args_num >= len(sc.configs) {
		actual_args_num = len(sc.configs) - 1
	}
	
	index, _, isLeader := sc.rf.Start(Op{OpType: "Query", Num: actual_args_num, ClientId: args.ClientId, OperationId: args.OperationId})
	
	if !isLeader{
		reply.Err = "NotLeader"
		return
	}
	// wait for committed
	alert_channel := make(chan raft.ApplyMsg, 1)

	existing_channel_list := make([]chan raft.ApplyMsg, 0)
	if sc.alert_channels[index] != nil {
		existing_channel_list = sc.alert_channels[index]
	}
	sc.alert_channels[index] = append(existing_channel_list, alert_channel)
	sc.mu.Unlock()
	//if committed, return value
	apply_msg := <-alert_channel
	sc.mu.Lock()
	sc.removeAlertChannel(index, alert_channel)

	if apply_msg.CommandIndex != index {
		panic("index mismatch")
	}

	if apply_msg.Command.(Op).OperationId != args.OperationId {
		reply.Err = "DifferentThingCommitted"
		return
	}
	value := sc.configs[actual_args_num]
	reply.Config = value

	delete(sc.last_oper, args.ClientId)
}

func (sc *ShardCtrler) applier() {
	for{
		msg := <-sc.applyCh
		sc.mu.Lock()
		if msg.CommandValid {
			op := msg.Command.(Op)
			fmt.Printf("%d: Apply %v\n", sc.me, op)
			if op.OpType == "ConfigChange" {
				if sc.last_oper[op.ClientId] != op.OperationId {
					if op.ConfigsIndex != len(sc.configs) {
						panic("Unexpected ConfigsIndex: " + strconv.Itoa(op.ConfigsIndex) + " vs " + strconv.Itoa(len(sc.configs)))
					}
					sc.configs = append(sc.configs, op.NewConfig)
					sc.last_oper[op.ClientId] = op.OperationId
					fmt.Printf("%d: At Config Idx %d New Config %v\n", sc.me, op.ConfigsIndex, op.NewConfig)
				}
			} else if op.OpType == "Query" {
				//do nothing
			} else {
				panic("Unexpected OpType: " + op.OpType)
			}
			sc.lastApplied = msg.CommandIndex

			// alert RPCs
			idx := msg.CommandIndex
			if sc.alert_channels[idx] != nil {
				for _, ch := range sc.alert_channels[idx] {
					ch <- msg
				}
			}
		} else {
			panic("Unexpected msg.CommandValid: " + strconv.FormatBool(msg.CommandValid))
		}
		sc.mu.Unlock()
	}
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.configs[0].Num = 0
	for i := 0; i < NShards; i++ {
		sc.configs[0].Shards[i] = 0
	}

	sc.alert_channels = make(map[int][]chan raft.ApplyMsg)
	sc.last_oper = make(map[int64]int64)

	go sc.applier()

	return sc
}


/*
duplicate operation should be replied with success (in this lab and last)
config changes should be made after committing

*/