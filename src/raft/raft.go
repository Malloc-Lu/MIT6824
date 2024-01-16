package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	// "index/suffixarray"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"6.824/src/labrpc"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type logEntry struct {
	index int						// * first index is 1 
	command interface{}
	term int
}

// * for 2A
type State struct {
	currentTerm int					// * latest term server has seen
	votedFor int 					// * candidateId that reveive vote in current term
	log []logEntry
	lastTimeFromLeader time.Time	// * record the time of latest hearing from the leader
	electionTimeOut int
	isLeader bool 					// * true means this server is the leader
}

type LeaderState struct {
	nextIndex []int 				// * index of next log entry to send to that server
	matchIndex []int 				// * index of highest log entry 
}

type ElectionChan struct {
	isLeader bool					// * means the current server has been elected to be leader
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// * chan to receive messeage
	applyCh chan ApplyMsg

	// * for log
	logger *zap.SugaredLogger
	// * for individual log
	loggerPrivate *zap.SugaredLogger

	// * for 2A
	state State						// * the State structure in figure 2
	electionChan chan ElectionChan
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	isleader = rf.state.isLeader
	term = rf.state.currentTerm
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int 					// * candidate's term
	CandidateId int 			// * candidate requesting vote
	LastLogIndex int       		// * index of candidate's last log entry
	LastLogTerm int 			// * term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int 					// * currentTerm, for candidate to update itself
	VoteGranted bool 			// * true means candidate received vote
}

type AppendEntriesArgs struct {
	Term int					// * leader's term
	LeaderId int 				// * so follower can redirect clients
	PrevLogIndex int			// * index of log entry immediately preceding new ones
	// Todo: other datas
	PrevLogTerm int 			// * term of prevLogIndex entry
	Entries []interface{}		// * log entries to store (empty for heartbeat)
	LeaderCommit int 			// * leader's commitIndex
}

type AppendEntriesReply struct {
	Term int 					// * currentTerm, for leader to update itself
	Success bool				// * true if follower contained entry matching prevLogIndex and prevLogTerm
}


//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// logger, _ := zap.NewProduction()
	// defer logger.Sync()
	// sugar := logger.Sugar()
	// sugar.Infof("use zap log")

	// Your code here (2A, 2B).
	rf.mu.Lock()
	
	// * print some information
	str := "enter the `rf.Requestvote()` handler\n" + 
			"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.me is %v\n" +
			"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.currentTerm is %v\n" +
			"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.voteFor is %v\n" +
			"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\targs.CandiateId is %v"
	rf.logger.Infof(str, rf.me, rf.state.currentTerm, rf.state.votedFor, args.CandidateId)
	rf.loggerPrivate.Infof(str, rf.me, rf.state.currentTerm, rf.state.votedFor, args.CandidateId)
	
	// * reset the election timeout
	rf.state.electionTimeOut = rf.getElectionTimeOut()
	rf.state.lastTimeFromLeader = time.Now()

	reply.Term = rf.state.currentTerm				// ! don't know the meaning of reply.Term
	if args.Term < rf.state.currentTerm {
		reply.VoteGranted = false					// * if `args.Term < rf.state.CurrentTerm`, return false immediately
		rf.mu.Unlock()
		return
	}
	// ! grant must if the candidate's log is at least as up-to-date as reveiver's log
	lastElemIndex := len(rf.state.log) - 1
	// * if logs have last entries with different terms, the log with later term is more up-to-date
	if rf.state.log[lastElemIndex].term != args.LastLogTerm {
		if args.LastLogTerm < rf.state.log[lastElemIndex].term {
			reply.VoteGranted = false
		}else {
			reply.VoteGranted = true
			// * modify `rf.state.votedFor` and update `rf.state.currentTerm`
			rf.state.votedFor = args.CandidateId
			rf.state.currentTerm = args.Term
		}
	} else {
		// * if logs end with the same term, whichever log is longer is more up-to-date
		if args.LastLogIndex < rf.state.log[lastElemIndex].index {
			reply.VoteGranted = false
		} else {
			reply.VoteGranted = true
			// * modify `rf.state.votedFor` and update `rf.state.currentTerm`
			rf.state.votedFor = args.CandidateId
			rf.state.currentTerm = args.Term
		}
	}

	// * print some information
	str = "before exiting the `rf.Requestvote()` handler\n" + 
			"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.me is %v\n" +
			"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.currentTerm is %v\n" +
			"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.voteFor is %v\n" +
			"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\targs.CandiateId is %v"
	rf.logger.Infof(str, rf.me, rf.state.currentTerm, rf.state.votedFor, args.CandidateId)
	rf.loggerPrivate.Infof(str, rf.me, rf.state.currentTerm, rf.state.votedFor, args.CandidateId)
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Success = true
	// * reveive AppendEntries RPC from new leader, convert to follower
	rf.state.isLeader = false
	
	// * update the `rf.lastTimeFromLeader`
	rf.state.electionTimeOut = rf.getElectionTimeOut()
	rf.state.lastTimeFromLeader = time.Now()
	// rf.logger.Infof("have update the last time from leader")
	// * reply false if term < currenTerm
	if args.Term < rf.state.currentTerm {
		reply.Success = false
	}
	rf.mu.Unlock()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	// rf.logger.Info("after rf.peers[server].Call() ok is %v", ok)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh
	// * use zap to log
	rf.logger = InitLogger("./workdir/raft")
	defer rf.logger.Sync()

	// * create a private logger
	para := fmt.Sprintf("./workdir/raft%v", me)
	rf.loggerPrivate = InitLogger(para)
	defer rf.loggerPrivate.Sync()

	rf.state.lastTimeFromLeader = time.Now()
	rf.state.electionTimeOut = rf.getElectionTimeOut()

	rf.state.currentTerm = 0
	rf.state.votedFor = -1
	rf.state.log = []logEntry{{0, nil, rf.state.currentTerm}}

	rf.logger.Info("\n\n---------------------------------------------Start a new server init-------------------------------------------")

	// Your initialization code here (2A, 2B, 2C).
	// * leader election
	rf.state.isLeader = false
	rf.electionChan = make(chan ElectionChan)
	go rf.LeaderElection(me)
	go rf.Convert2Leader(me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

// * return true means the server is elected to be leader
func (rf *Raft) LeaderElection(me int) bool {
	for {
		// todo: might can use RWMutex
		// * read the value of `rf.state.isLeader`
		rf.mu.Lock()
		isLeader := rf.state.isLeader
		rf.mu.Unlock()
		if !isLeader {
			time.Sleep(2 * time.Millisecond)
			rf.mu.Lock()
			interval := time.Now().Sub(rf.state.lastTimeFromLeader).Milliseconds()
			isElecTimeOut := interval > int64(rf.state.electionTimeOut)
			currTerm := rf.state.currentTerm
			rf.mu.Unlock()
			if isElecTimeOut {
				str := "the election timeout is %v\n" + 
						"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tthe distance between now and last time from leader is %vms\n" + 
						"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.state.isleader is %v\n" + 
						"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.state.currTerm is %v\n" + 
						"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tleader election's me is %v\n" + 
						"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tbegin to elect"
				rf.logger.Infof(str, rf.state.electionTimeOut, interval, rf.state.isLeader, me)
				rf.loggerPrivate.Infof(str, rf.state.electionTimeOut, interval, rf.state.isLeader, currTerm, me)

				rf.Convert2Candidate(me)
			}
		}
	}
}

// * return true means the server is elected to be leader
func (rf *Raft) Convert2Candidate(me int) bool {
	rf.mu.Lock()
	// * convert to candidate
	rf.state.currentTerm++
	// * vote for self
	rf.state.votedFor = me
	// * get election timeout range 300-450ms and update the `rf.state.electionTimeOut` (reset  election timer)
	rf.state.electionTimeOut = rf.getElectionTimeOut()
	// * reset the last time from the leader
	rf.state.lastTimeFromLeader = time.Now()
	// send rpc message
	lastElemIndex := len(rf.state.log) - 1
	rf.logger.Infof("rf.currenTerm is %v, rf.lastLogIndex is %v, rf.lastLogTerm is %v, me is %v", 
							rf.state.currentTerm, rf.state.log[lastElemIndex].index, rf.state.log[lastElemIndex].term, me)
	rf.loggerPrivate.Infof("rf.currenTerm is %v, rf.lastLogIndex is %v, rf.lastLogTerm is %v, me is %v", 
							rf.state.currentTerm, rf.state.log[lastElemIndex].index, rf.state.log[lastElemIndex].term, me)
	requestvoteargs := RequestVoteArgs{rf.state.currentTerm, me, rf.state.log[lastElemIndex].index, rf.state.log[lastElemIndex].term}
	requestvotereply := RequestVoteReply{}
	rf.mu.Unlock()
	votes := 1
	
	// ! should send request vote parallelly
	voteChan := make(chan bool)										// * used to notify whether the followers votes
	// * send RequestVote RPCs to all other servers
	for i := 0; i < len(rf.peers); i++ {
		if i != me{
			go func (server int, args *RequestVoteArgs, reply *RequestVoteReply) {
				if rf.sendRequestVote(server, args, reply) {
					rf.logger.Infof("Convert2Candidate.me is %v", me)
					rf.loggerPrivate.Infof("Convert2Candidate.me is %v", me)
					voteChan <- reply.VoteGranted
					// if reply.VoteGranted {
					// 	voteChan <- true
					// }
				} else {
					voteChan <- false
					rf.logger.Infof("rf.sendRequestVote failed when send from server %v to server %v", me, server)
					rf.loggerPrivate.Infof("rf.sendRequestVote failed when send from server %v to server %v", me, server)
				}
			}(i, &requestvoteargs, &requestvotereply)
		}
		// if rf.sendRequestVote(i, &requestvoteargs, &requestvotereply) {
		// 	rf.logger.Infof("Convert2Candidate.me is %v", me)
		// 	if requestvotereply.VoteGranted {
		// 		votes++
		// 		rf.logger.Infof("votes is %v, me is %v", votes, me)
		// 	}
		// }
	}
	totalVotes := 0								// * judge if all rf.sendRequestvote return
	for isVote := range voteChan {
		totalVotes++
		if isVote {
			votes++
			if votes > (len(rf.peers) / 2) {
				rf.mu.Lock()
				rf.state.isLeader = true
				// close(voteChan)
				rf.logger.Infof("the leader is %v", rf.me)
				rf.loggerPrivate.Infof("the leader is %v", rf.me)
				rf.mu.Unlock()
			} else {
				rf.mu.Lock()
				rf.state.isLeader = false
				rf.mu.Unlock()
			}
		}
		// * when all rf.sendRequestVote return, close the channel
		if totalVotes == (len(rf.peers) - 1) {
			close(voteChan)
		}
	}
	rf.logger.Info("------a new leaderElection() call-------")
	rf.loggerPrivate.Info("------a new leaderElection() call-------")


	// if votes > (len(rf.peers) / 2) {
	// 	// rf.electionChan <- ElectionChan{true}
	// 	rf.mu.Lock()
	// 	rf.logger.Infof("len(rf.peers) is %v, the votes number is greater than len(rf.peers) / 2", len(rf.peers))
	// 	rf.state.isLeader = true
	// 	rf.mu.Unlock()
	// 	}else {
	// 		// rf.electionChan <- ElectionChan{false}
	// 		rf.mu.Lock()
	// 		rf.state.isLeader = false
	// 		rf.mu.Unlock()
	// 	}
	return rf.state.isLeader
}

func (rf *Raft) Convert2Leader(me int) {
	for {
		// * read the state of `rf.state.isLeader`
		rf.mu.Lock()
		isLeader := rf.state.isLeader
		lastElemIndex := len(rf.state.log) - 1
		prevLogIndex := rf.state.log[lastElemIndex].index
		prevLogTerm := rf.state.log[lastElemIndex].term
		rf.mu.Unlock()

		sendFaults := 0								// * record times of sending failure
		if isLeader {
			appendEntriesArgs := AppendEntriesArgs{rf.state.currentTerm, me, prevLogIndex, prevLogTerm, nil, 0} 		// todo: data in the structure need to modify
			appendEntriesReply := AppendEntriesReply{0, false}
			for i := 0; i < len(rf.peers); i++ {
				if i != me {
					if rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply) {
						if appendEntriesReply.Success {
							rf.mu.Lock()
							nowIsLeader := rf.state.isLeader
							rf.mu.Unlock()
							if isLeader != nowIsLeader{
								rf.logger.Infof("server %v come back to follower from leader", rf.me)
								rf.loggerPrivate.Infof("server %v come back to follower from leader", rf.me)
							}
							// rf.logger.Infof("me is %v, i is %v", me, i)
						}
					} else {
						sendFaults++
						// * fail to receive reply from majority servers
						if sendFaults > (len(rf.peers) / 2) {
							rf.logger.Infof("fail to receive reply from majority servers, rf.me is %v, sendFaults is %v", me, sendFaults)
							rf.loggerPrivate.Infof("fail to receive reply from majority servers, rf.me is %v, sendFaults is %v", me, sendFaults)
							rf.mu.Lock()
							rf.state.isLeader = false
							rf.mu.Unlock()
						}
					}

				}
			}
		}
		// * periodically send heartbeat with setting the period is 120ms
		time.Sleep(120 * time.Millisecond)
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

func (rf *Raft) getElectionTimeOut() int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(150) + 300
}