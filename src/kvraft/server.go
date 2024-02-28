package kvraft

import (
	// "internal/itoa"
	"log"
	"sync"
	"sync/atomic"

	"6.824/src/labgob"
	"6.824/src/labrpc"
	"6.824/src/raft"

	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Operation int
// * enum types of operation
const (
	GetOp Operation = iota
	PutOp
	AppendOp
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string	 					// * operation type	
	Key string
	Value string
	Identity int						// * only value to identity operation
}

type Done struct {
	Ev string							// * error message of wrong command execution if identity equals -1, or value
	Identity int						// * only id of command
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	logger *zap.SugaredLogger
	latestIndex int 					// * record the highest index of command that known to be handled
	database map[string]string			// * define a key/value database
	done chan Done						// * inform complished command, -1 means failure
	isDoneAlive bool					// * if true, send message to channal; or, don't send
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	op := Op{"Get", args.Key, "", args.Identity}
	kv.logger.Infof("\n----------------------Get(%v) handler begins----------------", op)	
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "This KVServer is not Leader"
		reply.Value = ""
		kv.logger.Infof("\n----------------------Get(%v) handler ends----------------\n", op)	
		kv.mu.Unlock()
		return
	}
	// * make Channal kv.done alive
	kv.isDoneAlive = true
	// * listen Channal `kv.Done`
	select {
	case m := <-kv.done:
		kv.logger.Infof("\nIn Get()\n, receive from kv.done, m is %v, op is %v\n", m, op)
		if m.Identity == op.Identity {
			reply.Value = m.Ev
			reply.Err = ""
		} else if m.Identity == -1 {
			reply.Value = ""
			reply.Err = Err(m.Ev)
		} else {
			err := fmt.Sprintf("\nm.identity is %v, op.identity is %v", m.Identity, op.Identity)
			reply.Err = Err(err)
			kv.logger.Errorf("\nm.identity is %v, op.identity is %v", m.Identity, op.Identity)	
		}
	case <-time.After(100 * time.Millisecond):
		kv.logger.Errorf("\nIn Get(), timeout while listening kv.done, op is %v\n", op)
	}
	// * make Channal kv.done offline
	kv.isDoneAlive = false
	kv.logger.Infof("\n----------------------Get(%v) handler ends----------------\n", op)
	kv.mu.Unlock()
	return

	// if valid, str := kv.ListenAndHandle(op); valid {
	// 	kv.logger.Infof("valid is %v, str is %v", valid, str)
	// 	reply.Err = ""
	// 	reply.Value = str
	// 	return
	// } else {
	// 	reply.Err = Err(str)
	// 	reply.Value = ""
	// 	return
	// }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	op := Op{args.Op, args.Key, args.Value, args.Identity}
	kv.logger.Infof("\n----------------------PutAppend(%v) handler begins----------------", op)
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "This KVServer is not Leader"
		kv.logger.Infof("\n----------------------PutAppend(%v) handler ends----------------\n", op)
		kv.mu.Unlock()
		return
	}
	// * listen Channal `kv.Done`
	// * make Channal kv.done alive
	kv.isDoneAlive = true
	select {
	case m := <-kv.done:
		if m.Identity == op.Identity {
			reply.Err = ""
		} else if m.Identity == -1 {
			reply.Err = Err(m.Ev)
		} else {
			err := fmt.Sprintf("\nm.identity is %v, op.identity is %v", m.Identity, op.Identity)
			reply.Err = Err(err)
			kv.logger.Errorf("\nm.identity is %v, op.identity is %v", m.Identity, op.Identity)
		}
	case <-time.After(100 * time.Millisecond):
		kv.logger.Errorf("\nIn PutAppend(), timeout while listening kv.done, op is %v\n", op)
	}
	// * make Channal kv.done offline	
	kv.isDoneAlive = false
	kv.logger.Infof("\n----------------------PutAppend(%v) handler ends----------------\n", op)
	kv.mu.Unlock()
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	fmt.Printf("servers is %v, me is %v\n", servers, me)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	// * build logger
	para := fmt.Sprintf("./workdir/server%v", me)
	kv.logger = InitLogger(para) 

	// * initiate some value
	kv.latestIndex = 0
	kv.isDoneAlive = false
	kv.database = make(map[string]string)

	// * initiate `Done` channal
	kv.done = make(chan Done)

	// * listen Channal `kv.applyCh`
	go kv.ListenAndHandle1()	

	return kv
}

// func (kv *KVServer) ListenAndHandle(op Op) (bool, []string) {
// 	kv.logger.Infof("\nkv.applyCh is %v\n", kv.applyCh)
// 	select {
// 	case msg := <- kv.applyCh:
// 		kv.logger.Infof("\nmsg is %v\n", msg)
// 		if op, ok := msg.Command.(Op); ok {					// * `msg.Command.(Op)` is `type assertion`
// 			if op.OpType == "Get" {
// 				if value, ok := kv.database[op.Key]; ok {
// 					return true, value
// 				} else {
// 					return true, []string{""}
// 				}
// 			}
// 		}
		
// 	case <- time.After(100 * time.Millisecond):
// 		kv.logger.Infof("\ntimeout while listening operation %v\n", op)
// 		err := fmt.Sprintf("\ntimeout while listening operation %v\n", op)
// 		return false, []string{err}
// 	}

// 	return false, []string{"unexpected return"}
// }

func (kv *KVServer) ListenAndHandle1() (bool, string) {
	times := 0
	for msg := range kv.applyCh {
		times++
		kv.logger.Infof("\n======================round %v ListenAndHandle1() begins=================", times)
		// * successfully commit the command
		// kv.mu.Lock()
		str := "\nIn ListenAndHandle1()\n" + 
				"receive from kv.applyCh, msg is %v\n"
		kv.logger.Infof(str, msg)
		if op, ok := msg.Command.(Op); ok {					// * `msg.Command.(Op)` is `type assertion`
			if op.OpType == "Get" {
				kv.get(op)
				kv.logger.Infof("after calling kv.get(%v)", op)
			} else if op.OpType == "Put" {
				kv.put(op)
			} else if op.OpType == "Append" {
				kv.append(op)
			}
		}	
		kv.logger.Infof("\n=====================round %v ListenAndHandle1() ends===================\n", times)
		// kv.mu.Unlock()
	}

	// for {
	// 	select {
	// 	case msg := <- kv.applyCh:
	// 		kv.logger.Infof("msg is %v", msg)
	// 		if op, ok := msg.Command.(Op); ok {					// * `msg.Command.(Op)` is `type assertion`
	// 			if op.OpType == GetOp {
	// 				kv.done <- Done{"", op.Identity}
	// 			} else if op.OpType == PutOp {
	// 				kv.done <- Done{"", op.Identity}
	// 			}
	// 		}
			
	// 	case <- time.After(100 * time.Millisecond):
	// 		kv.logger.Infof("timeout while listening operation %v", op)
	// 		err := fmt.Sprintf("timeout while listening operation %v", op)
	// 		return false, err
	// 	}
	// }
	kv.logger.Errorf("\nlong Go routine bad return")

	return false, "unexpected return"
}

func (kv *KVServer) get(op Op) {
	kv.logger.Infof("\nIn get()\nop is %v, op.identity is %v", op, op.Identity)
	if value, ok := kv.database[op.Key]; ok {
		if kv.isDoneAlive {
			kv.done <- Done{value, op.Identity}
		}
	} else {
		if kv.isDoneAlive {
			kv.done <- Done{"", op.Identity}
		}
	}
}

func (kv *KVServer) put(op Op) {
	// * directly replace value of `op.key`
	value := kv.database[op.Key]
	kv.database[op.Key] = op.Value
	kv.logger.Infof("op.value is %v, op.identity is %v, kv.database is %v", op.Value, op.Identity, kv.database)
	kv.logger.Infof("\nIn ListenAndHandle()\n have modified kv.database[%v] from %v To %v\n, op is %v\n", op.Key, value, kv.database[op.Key], op)
	// ! not every KVServer have Channal kv.done
	if kv.isDoneAlive {
		kv.done <- Done{"", op.Identity}
	}
}

func (kv *KVServer) append(op Op) {
	if value, ok := kv.database[op.Key]; !ok {
		kv.database[op.Key] = op.Value
		kv.logger.Infof("\nIn ListenAndHandle(), Append\n kv.database has NOT key(%v), make kv.database[%v] = %v\n", op.Key, op.Key, kv.database[op.Key])
		if kv.isDoneAlive {
			kv.done <- Done{"", op.Identity}
		}
	} else {
		kv.database[op.Key] = value + op.Value
		kv.logger.Infof("\nIn ListenAndHandle(), Append\n kv.database has the key(%v), make kv.database[%v] = %v\n", op.Key, op.Key, kv.database[op.Key])
		if kv.isDoneAlive {
			kv.done <- Done{"", op.Identity}
		}
	}
}

func InitLogger(prefix string) *zap.SugaredLogger {
	// get the current date
	currentTime := time.Now()
	date := fmt.Sprintf("%v%v%v", currentTime.Year(), currentTime.Format("01"), currentTime.Format("02"))
	// logName = fmt.Sprintf("./workdir/raft%v.log", date)
	file, _ := os.OpenFile(prefix + date + ".log", os.O_RDWR | os.O_CREATE | os.O_APPEND, 0755)
	writeSyncer := zapcore.AddSync(file)
	// encoder := zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
	// encoder := zapcore.NewConsoleEncoder(zap.NewProductionEncoderConfig())
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	encoder := zapcore.NewConsoleEncoder(encoderConfig)

	core := zapcore.NewCore(encoder, writeSyncer, zapcore.DebugLevel)

	logger := zap.New(core, zap.AddCaller())
	sugarLogger := logger.Sugar()
	return sugarLogger
}

// \t\t\t\t\t\t\t\t\t\t\t\t\t\t\t