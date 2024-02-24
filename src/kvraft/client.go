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
	lastLeader int				// * record index of last leader that Clerk found
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
	
	for {
		for i := 0; i < 5; i++ {
			// ! remember to use individual `args` and `reply`
			args := GetArgs{key, int(ck.identity)}
			reply := GetReply{}
			if ok := ck.servers[i].Call("KVServer.Get", &args, &reply); ok {
				if reply.Err == "" {
					ck.lastLeader = i
					ck.logger.Infof("i is %v, getReply.Err is %v, getReply.Value is %v, ck.lastLeader is %v", i, reply.Err, reply.Value, ck.lastLeader)
					return reply.Value
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
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
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
