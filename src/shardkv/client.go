package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	client_id int64
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.client_id = nrand()
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	operation_id := nrand()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])

				return_channel := make(chan GetReply, 1)
				timeout := make(chan bool, 1)

				go func() {
					args := GetArgs{Key: key, ClientId: ck.client_id, OperationId: operation_id}
					reply_copy := GetReply{}
					ok := srv.Call("ShardKV.Get", &args, &reply_copy)
					if ok {
						return_channel <- reply_copy
					} else {
						return_channel <- GetReply{Err: "BadCall"}
					}
				}()
				go func(){
					time.Sleep(150 * time.Millisecond)
					timeout <- true
				}()

				reply := GetReply{Err: "Null"}
				select {
					case a := <- return_channel:
						// a read from ch has occurred
						reply = a
					case <-timeout:
						reply = GetReply{Err: "Timeout"}
				}

				if reply.Err == "" { //removed reply.Err == "ErrNoKey"
					return reply.Value
				}
				if reply.Err == "ErrWrongGroup" {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	operation_id := nrand()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])

				return_channel := make(chan PutAppendReply, 1)
				timeout := make(chan bool, 1)

				go func() {
					args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.client_id, OperationId: operation_id}
					reply_copy := PutAppendReply{}
					ok := srv.Call("ShardKV.PutAppend", &args, &reply_copy)
					if ok {
						return_channel <- reply_copy
					} else {
						return_channel <- PutAppendReply{Err: "BadCall"}
					}
				}()
				go func(){
					time.Sleep(150 * time.Millisecond)
					timeout <- true
				}()

				reply := PutAppendReply{Err: "Null"}
				select {
					case a := <- return_channel:
						// a read from ch has occurred
						reply = a
					case <-timeout:
						reply = PutAppendReply{Err: "Timeout"}
				}

				if reply.Err == "" {
					return
				}
				if reply.Err == "ErrWrongGroup" {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controller for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
