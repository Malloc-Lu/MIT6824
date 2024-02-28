package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync/atomic"
	"time"

	"6.824/src/labrpc"
	"go.uber.org/zap"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	logger *zap.SugaredLogger

	identity int32 				// * identify the only operation
	lastLeader int32			// * record index of last leader that Clerk found
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
	// You'll have to add code here.
	ck.logger = InitLogger("./workdir/client")
	ck.identity = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	// * upgrade `ck.identiy`
	atomic.AddInt32(&ck.identity, 1)
	times := 0
	i := atomic.LoadInt32(&ck.lastLeader)
	
	for {
		times++
		ck.logger.Infof("\n----------------round %v Get(%v) begins-----------------", times, ck.identity)
		// ! remember to use individual `args` and `reply`
		args := GetArgs{key, int(ck.identity)}
		reply := GetReply{}
		if ok := ck.servers[i].Call("KVServer.Get", &args, &reply); ok {
			if reply.Err == "" {
				if i != atomic.LoadInt32(&ck.lastLeader) {
					atomic.StoreInt32(&ck.lastLeader, int32(i))
					ck.logger.Infof("\n Leader of KVServer may change to %v", i)
				}
				ck.logger.Infof("\nSuccessfully Get\ni is %v, GetReply.Err is %v, ck.lastLeader is %v", i, reply.Err, atomic.LoadInt32(&ck.lastLeader))
				ck.logger.Infof("\n----------------round %v Get(%v) ends-----------------\n", times, ck.identity)
				return reply.Value
			} else {
				ck.logger.Errorf("\nWrong Get\ni is %v, GetReply.Err is %v, ck.lastLeader is %v", i, reply.Err, atomic.LoadInt32(&ck.lastLeader))
				i = int32((int(i) + 1) % len(ck.servers))
			}
		}
		if times % len(ck.servers) == 0 {
			time.Sleep(10 * time.Millisecond)
		}
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	times := 0
	atomic.AddInt32(&ck.identity, 1)
	i := atomic.LoadInt32(&ck.lastLeader)
	for {
		times++
		ck.logger.Infof("\n==================round %v PutAppend(%v) op(%v) begins==================", times, ck.identity, op)	
		ck.logger.Infof("\nkey is %v, value is %v", key, value)
		
		// * construct args and reply structure to send rpc
		args := PutAppendArgs{key, value, op, int(ck.identity)}
		reply := PutAppendReply{}

		// * send RPC to KVServer
		if ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply); ok {
			if reply.Err == ""{
				if i != atomic.LoadInt32(&ck.lastLeader) {
					atomic.StoreInt32(&ck.lastLeader, int32(i))
					ck.logger.Infof("\n Leader of KVServer may change to %v", i)
				}
				ck.logger.Infof("\nSuccessully PutAppend\ni is %v, PutAppendReply.Err is %v, ck.lastLeader is %v", i, reply.Err, atomic.LoadInt32(&ck.lastLeader))
				ck.logger.Infof("\n==================round %v PutAppend(%v) op(%v) ends==================\n", times, ck.identity, op)
				return
			} else {
				ck.logger.Errorf("\nWrong PutAppend\ni is %v, PutAppendReply.Err is %v, ck.lastLeader is %v", i, reply.Err, atomic.LoadInt32(&ck.lastLeader))
				i = int32((int(i) + 1) % len(ck.servers))
			}
		}
		if times % len(ck.servers) == 0 {
			time.Sleep(10 * time.Millisecond)
		}

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
