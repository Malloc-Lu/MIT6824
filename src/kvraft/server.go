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
	OpType Operation 					// * operation type	
	Key string
	Value string
	Identity int						// * only value to identity operation
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
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{GetOp, args.Key, "", args.Identity}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = "This Server is not Leader"
		reply.Value = ""
		return
	}
	// * listen Channal `kv.applyCh`
	if valid, str := kv.ListenAndHandle(op); valid {
		kv.logger.Infof("valid is %v, str is %v", valid, str)
		reply.Err = ""
		reply.Value = str
		return
	} else {
		reply.Err = Err(str)
		reply.Value = ""
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
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
	kv.database = make(map[string]string)

	// * listen Channal `kv.applyCh`
	// go func ()  {
	// 	select {
	// 	case msg := <- kv.applyCh:

	// 	}	
	// }

	return kv
}

func (kv *KVServer) ListenAndHandle(op Op) (bool, string) {
	kv.logger.Infof("kv.applyCh is %v", kv.applyCh)
	select {
	case msg := <- kv.applyCh:
		kv.logger.Infof("msg is %v", msg)
		if op, ok := msg.Command.(Op); ok {					// * `msg.Command.(Op)` is `type assertion`
			if op.OpType == GetOp {
				if value, ok := kv.database[op.Key]; ok {
					return true, value
				} else {
					return true, ""
				}
			}
		}
		
	case <- time.After(100 * time.Millisecond):
		kv.logger.Infof("timeout while listening operation %v", op)
		err := fmt.Sprintf("timeout while listening operation %v", op)
		return false, err
	}

	return false, "unexpected return"

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