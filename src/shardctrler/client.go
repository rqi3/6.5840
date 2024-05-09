package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	client_id int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.client_id = nrand()
	return ck
}

func (ck *Clerk) Query(num int) Config {
	operation_id := nrand()
	possible_leader := 0
	for {
		// fmt.Printf("Shardcontroller client getting possible leader: %d\n", possible_leader)
		args := QueryArgs{
			Num: num,
			ClientId: ck.client_id,
			OperationId: operation_id,
		}
		reply := QueryReply{}
		ok := ck.servers[possible_leader].Call("ShardCtrler.Query", &args, &reply)

		if !ok || reply.Err != "" {
			// fmt.Printf("Shardctrler Client Err: %s\n", reply.Err)
			possible_leader = (possible_leader + 1) % len(ck.servers)
			continue
		}
		// fmt.Printf("Shardctrler Success\n")
		return reply.Config//success!
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	operation_id := nrand()
	possible_leader := 0
	for {
		return_channel := make(chan JoinReply, 1)
		timeout := make(chan bool, 1)
		go func(leader int) {
			args := JoinArgs{
				Servers: servers,
				ClientId: ck.client_id,
				OperationId: operation_id,
			}
			reply_copy := JoinReply{}
			ok := ck.servers[leader].Call("ShardCtrler.Join", &args, &reply_copy)
			if ok {
				return_channel <- reply_copy
			} else {
				return_channel <- JoinReply{Err: "BadCall"}
			}
		}(possible_leader)
		go func(){
			time.Sleep(150 * time.Millisecond)
			timeout <- true
		}()

		reply := JoinReply{Err: "Null"}
		select {
			case a := <- return_channel:
				// a read from ch has occurred
				reply = a
			case <-timeout:
				reply = JoinReply{Err: "Timeout"}
		}

		if reply.Err != "" {
			possible_leader = (possible_leader + 1) % len(ck.servers)
			continue
		}
		// fmt.Printf("Success %s %s %s\n", key, value, op)
		return //success!
	}
}

func (ck *Clerk) Leave(gids []int) {
	operation_id := nrand()
	possible_leader := 0
	for {
		return_channel := make(chan LeaveReply, 1)
		timeout := make(chan bool, 1)
		go func(leader int) {
			args := LeaveArgs{
				GIDs: gids,
				ClientId: ck.client_id,
				OperationId: operation_id,
			}
			reply_copy := LeaveReply{}
			ok := ck.servers[leader].Call("ShardCtrler.Leave", &args, &reply_copy)
			if ok {
				return_channel <- reply_copy
			} else {
				return_channel <- LeaveReply{Err: "BadCall"}
			}
		}(possible_leader)
		go func(){
			time.Sleep(150 * time.Millisecond)
			timeout <- true
		}()

		reply := LeaveReply{Err: "Null"}
		select {
			case a := <- return_channel:
				// a read from ch has occurred
				reply = a
			case <-timeout:
				reply = LeaveReply{Err: "Timeout"}
		}

		if reply.Err != "" {
			possible_leader = (possible_leader + 1) % len(ck.servers)
			continue
		}
		// fmt.Printf("Success %s %s %s\n", key, value, op)
		return //success!
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	operation_id := nrand()
	possible_leader := 0
	for {
		return_channel := make(chan MoveReply, 1)
		timeout := make(chan bool, 1)
		go func(leader int) {
			args := MoveArgs{
				Shard: shard,
				GID: gid,
				ClientId: ck.client_id,
				OperationId: operation_id,
			}
			reply_copy := MoveReply{}
			ok := ck.servers[leader].Call("ShardCtrler.Move", &args, &reply_copy)
			if ok {
				return_channel <- reply_copy
			} else {
				return_channel <- MoveReply{Err: "BadCall"}
			}
		}(possible_leader)
		go func(){
			time.Sleep(150 * time.Millisecond)
			timeout <- true
		}()

		reply := MoveReply{Err: "Null"}
		select {
			case a := <- return_channel:
				// a read from ch has occurred
				reply = a
			case <-timeout:
				reply = MoveReply{Err: "Timeout"}
		}

		if reply.Err != "" {
			possible_leader = (possible_leader + 1) % len(ck.servers)
			continue
		}
		// fmt.Printf("Success %s %s %s\n", key, value, op)
		return //success!
	}
}
