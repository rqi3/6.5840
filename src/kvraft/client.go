package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	possible_leader int // index of the leader, maybe
	client_id int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// func randStr() string {
// 	num := nrand()
// 	return strconv.FormatInt(num, 10)
// }

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.possible_leader = 0
	ck.client_id = nrand()
	
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	// fmt.Printf("Get %s\n", key)
	operation_id := nrand()
	
	for{
		args := GetArgs{Key: key, ClientId: ck.client_id, OperationId: operation_id}
		reply := GetReply{}
		ok := ck.servers[ck.possible_leader].Call("KVServer.Get", &args, &reply)

		if !ok || reply.Err != "" {
			ck.possible_leader = (ck.possible_leader + 1) % len(ck.servers)
			continue
		}

		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	// fmt.Printf("PutAppend %s %s %s\n", key, value, op)
	operation_id := nrand()
	
	for{
		args := PutAppendArgs{Key: key, Value: value, ClientId: ck.client_id, OperationId: operation_id}
		reply := PutAppendReply{}
		ok := ck.servers[ck.possible_leader].Call("KVServer." + op, &args, &reply)

		if !ok || reply.Err != "" {
			ck.possible_leader = (ck.possible_leader + 1) % len(ck.servers)
			continue
		}
		// fmt.Printf("Success %s %s %s\n", key, value, op)
		return //success!
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
