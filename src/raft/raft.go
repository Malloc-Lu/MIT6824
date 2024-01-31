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
	"math"
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

type LogEntry struct {
	Index int						// * first index is 1 
	Command interface{}
	Term int
}

// * for 2A
type State struct {
	currentTerm int					// * latest term server has seen
	votedFor int 					// * candidateId that reveive vote in current term
	log []LogEntry
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

	// * for 2B
	leaderstate LeaderState
	commitIndex int 				// * index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int 				// * index of highest log entry applied to state machine (initialized to 0, increases monotonically)
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
	Entries []LogEntry			// * log entries to store (empty for heartbeat)
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
			"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\targ.Term is %v\n" +
			"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.voteFor is %v\n" +
			"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\targs.CandiateId is %v"
	// rf.logger.Infof(str, rf.me, rf.state.currentTerm, rf.state.votedFor, args.CandidateId)
	rf.loggerPrivate.Infof(str, rf.me, rf.state.currentTerm, args.Term, rf.state.votedFor, args.CandidateId)
	
	// * reset the election timeout
	rf.state.electionTimeOut = rf.getElectionTimeOut()
	rf.state.lastTimeFromLeader = time.Now()

	reply.Term = rf.state.currentTerm				// ! don't know the meaning of reply.Term
	if args.Term < rf.state.currentTerm {
		rf.loggerPrivate.Infof("args.Term is %v, rf.state.currentTerm is %v", args.Term, rf.state.currentTerm)
		reply.VoteGranted = false					// * if `args.Term < rf.state.CurrentTerm`, return false immediately
		rf.mu.Unlock()
		return
	}
	// ! grant must if the candidate's log is at least as up-to-date as reveiver's log
	lastElemIndex := len(rf.state.log) - 1
	// * if logs have last entries with different terms, the log with later term is more up-to-date
	if rf.state.log[lastElemIndex].Term != args.LastLogTerm {
		if args.LastLogTerm < rf.state.log[lastElemIndex].Term {
			rf.loggerPrivate.Infof("args.LastLogTerm is %v, rf.state.log[LastElemIndex].Term is %v", args.LastLogTerm, rf.state.log[lastElemIndex].Term)
			reply.VoteGranted = false
		}else {
			reply.VoteGranted = true
			// * modify `rf.state.votedFor` and update `rf.state.currentTerm`
			rf.state.votedFor = args.CandidateId
			rf.state.currentTerm = args.Term
		}
	} else {
		// * if logs end with the same term, whichever log is longer is more up-to-date
		if args.LastLogIndex < rf.state.log[lastElemIndex].Index {
			rf.loggerPrivate.Infof("args.LastLogIndex is %v, rf.state.log[lastElemIndex].Index is %v", args.LastLogIndex, rf.state.log[lastElemIndex].Index)
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
	// rf.logger.Infof(str, rf.me, rf.state.currentTerm, rf.state.votedFor, args.CandidateId)
	rf.loggerPrivate.Infof(str, rf.me, rf.state.currentTerm, rf.state.votedFor, args.CandidateId)
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// * judge if it is a HeartBeat
	rf.mu.Lock()
	if args.Entries == nil {

		reply.Term = rf.state.currentTerm

		// * reply false if term < currenTerm
		if args.Term < rf.state.currentTerm {
			reply.Success = false
			rf.loggerPrivate.Infof("reply.Success is %v, args.Term is %v, rf.state.currentTerm is %v", reply.Success, args.Term, rf.state.currentTerm)
			// ! to Unlock!!!
			rf.mu.Unlock()
			return
		}

		lastElemIndex := len(rf.state.log) - 1
		if lastElemIndex < args.PrevLogIndex {
			reply.Success = false
			rf.loggerPrivate.Infof("reply.Success is %v, lastElemIndex is %v, args.PrevLogIndex is %v", reply.Success, lastElemIndex, args.PrevLogIndex)
			rf.mu.Unlock()
			return
		}
		if rf.state.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			rf.loggerPrivate.Infof("reply.Success is %v, rf.state.log[args.PrevLogIndex].Term is %v, args.PrevLogTerm is %v",reply.Success, rf.state.log[args.PrevLogIndex].Term, args.PrevLogTerm)
			rf.mu.Unlock()
			return
		}

		reply.Success = true
		// * reveive AppendEntries RPC from new leader, convert to follower
		rf.state.isLeader = false
		rf.loggerPrivate.Infof("AppendEntries() converts rf.isLeader to false")
		
		// * update the `rf.lastTimeFromLeader`
		rf.state.electionTimeOut = rf.getElectionTimeOut()
		rf.state.lastTimeFromLeader = time.Now()
		// rf.logger.Infof("have update the last time from leader")
		
		rf.loggerPrivate.Infof("args.Entries == nil, args.LeaderCommit is %v, rf.commitIndex is %v", args.LeaderCommit, rf.commitIndex)
		// * are there any log entries need to be committed?
		if args.LeaderCommit != rf.commitIndex {
			// * there is no possibility that `args.LeaderCommit < rf.commitIndex` in my understand
			// * if `args.LeaderCommit > rf.commitIndex` set commitIndex min
			// ! assume that `args.LeaderCommit > rf.commitIndex`
			rf.logger.Infof("args.LeaderCommit is %v, rf.commitIndex is %v", args.LeaderCommit, rf.commitIndex)
			if args.LeaderCommit > rf.commitIndex {
				oldCommitIndex := rf.commitIndex
				// * set commitIndex min
				rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.state.log) - 1)))
				rf.loggerPrivate.Infof("oldCommitIndex is %v, rf.commitIndex is %v", oldCommitIndex, rf.commitIndex)
				// * ensure the range of log entries being committed
				for _, entry := range rf.state.log[oldCommitIndex + 1 : rf.commitIndex + 1] {		// * pay attention to rule of spliting slice
					applymsg := ApplyMsg{true, entry.Command, entry.Index}
					rf.applyCh <- applymsg
					rf.loggerPrivate.Infof("server %v has committed the log entry %v", rf.me, applymsg)
				}
			}
		}
	} else {
		rf.logger.Infof("-----------------------------start appending entries--------------------------")

		str := "before AppendEntries() handler\n" + 
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.me is %v\n" + 
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.currTerm is %v\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.state.log is %v\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\targs.Term is %v\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\targs.LeaderId is %v\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\targs.PrevLogIndex is %v\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\targs.PrevLogTerm is %v\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\targs.Entries is %v\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\targs.LeaderCommit is %v\n"
		rf.logger.Infof(str, rf.me, rf.state.currentTerm, rf.state.log, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)
		rf.loggerPrivate.Infof(str, rf.me, rf.state.currentTerm, rf.state.log, args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.Entries, args.LeaderCommit)

		reply.Success = true
		reply.Term = rf.state.currentTerm

		lastElemIndex := len(rf.state.log) - 1
		var localPrevLogTerm int
		if lastElemIndex >= args.PrevLogIndex {
			localPrevLogTerm = rf.state.log[args.PrevLogIndex].Term
		}
		// * reply false if term < currenTerm
		if args.Term < rf.state.currentTerm {
			reply.Success = false
			rf.loggerPrivate.Infof("reply.Success is %v, args.Term is %v, rf.state.currentTerm is %v", reply.Success, args.Term, rf.state.currentTerm)
			rf.mu.Unlock()
			return
		}
		appendIndex := 0 			// ! record where to append the `args.entries`
		// * the length of `rf.state.log` is less than `args.PrevLogIndex`, return false
		if lastElemIndex < args.PrevLogIndex {
			reply.Success = false
			rf.loggerPrivate.Infof("reply.Success is %v, lastElemIndex is %v, args.PrevLogIndex is %v", reply.Success, lastElemIndex, args.PrevLogIndex)
		} else {
			// * entry at PrevLogIndex whose term doesn't match PrevLogTerm
			if lastElemIndex == args.PrevLogIndex && localPrevLogTerm != args.PrevLogTerm{
				reply.Success = false
				rf.loggerPrivate.Infof("reply.Success is %v, localPrevLogTerm is %v, args.PrevLogTerm is %v", reply.Success, localPrevLogTerm, args.PrevLogTerm)
			}
			// * exiting entry conflicts with a new one, delete the existing entry and all that follow it
			if lastElemIndex > args.PrevLogIndex {
				// * entry at `args.PrevLogIndex` whose term differs with `args.PrevLogTerm`, delete it and all that follow it
				if localPrevLogTerm != args.PrevLogTerm {
					rf.loggerPrivate.Infof("deleted log entries is %v", rf.state.log[args.PrevLogIndex :])
					rf.state.log = rf.state.log[:args.PrevLogIndex]			// * be cautious of rule of slice
					reply.Success = false
					rf.loggerPrivate.Infof("reply.Success is %v, localPrevLogTerm is %v, args.PrevLogTerm is %v", reply.Success, localPrevLogTerm, args.PrevLogTerm)
				}  
				// * entry at `args.PrevLogIndex` whose term equals to `args.PrevLogTerm`, only delete all that follow it
				isDele := false
				if localPrevLogTerm == args.PrevLogTerm {
					for entry_index, entry := range args.Entries {
						if entry.Index > lastElemIndex {
							appendIndex = entry_index
							break
						}
						eindex := entry.Index
						ecommand := entry.Command
						eterm := entry.Term
						if eindex != rf.state.log[eindex].Index || ecommand != rf.state.log[eindex].Command || eterm != rf.state.log[eindex].Term {
							isDele = true
							rf.loggerPrivate.Infof("isDele is %v, entry is %v, rf.state.log[%v] is %v", isDele, entry, eindex, rf.state.log[eindex])
							break
						}
					}
					if isDele {
						rf.loggerPrivate.Infof("deleted log entries is %v", rf.state.log[args.PrevLogIndex + 1 :])
						rf.state.log = rf.state.log[:args.PrevLogIndex + 1]
						// * if deleted, append `args.entries` from the beginning
						appendIndex = 0
					}
					// * reply.Success = true 								// * the value of reply.Success is default true
				}
				if !isDele && args.Entries[len(args.Entries) - 1].Index <= lastElemIndex {
					// * do not append any entries
					appendIndex = -1
				}
			}	
		}
		// * apend new entry if reply.Success is still true
		if reply.Success && appendIndex != -1{
			rf.logger.Infof("append a new entry, length of rf.log is %v, rf.me is %v", len(rf.state.log), rf.me)
			entriesToAppend := args.Entries[appendIndex : ]
			rf.state.log = append(rf.state.log, entriesToAppend...)

			// ! remember to send the committed 
		}
		// todo: if `args.LeaderCommit` > `rf.commitIndex`, set `rf.commitIndex` = min
		// if args.LeaderCommit > rf.commitIndex {
		// 	rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(len(rf.state.log) + 1)))
		// }

		str = "after the AppendEntries() handler\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\treply.Success is %v\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.state.log is %v\n" +
				"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\treply.Term is %v\n"
		rf.logger.Infof(str, reply.Success, rf.state.log, reply.Term)
		rf.loggerPrivate.Infof(str, reply.Success, rf.state.log, reply.Term)
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
	rf.loggerPrivate.Infof("sendAppendEntries()'s ok is %v", ok)
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

	rf.mu.Lock()
	isLeader = rf.state.isLeader
	// rf.mu.Unlock()

	if isLeader {
		rf.loggerPrivate.Infof("---------------------start part 2B (%v)-------------------", command)
		// Your code here (2B).
		// * construct a log entry
		// rf.mu.Lock()
		lastElemIndex := len(rf.state.log) - 1
		index = rf.state.log[lastElemIndex].Index + 1			// * the new log entry's index should be 1 greater than `LastElemIndex`
		term = rf.state.currentTerm
		isLeader = rf.state.isLeader

		// * read information for `AppendEntriesArgs`
		me := rf.me
		prevLogIndex := rf.state.log[rf.leaderstate.nextIndex[me] - 1].Index		// * the `prevLogIndex` should be the one immediately preceding the entry be about to send
		prevLogTerm := rf.state.log[prevLogIndex].Term
		leaderCommit := rf.commitIndex
		
		// * append the entry to `log[]` if the current server is leader
		logentry := LogEntry{index, command, term}
		rf.state.log = append(rf.state.log, logentry)
		rf.loggerPrivate.Infof("rf.leaderstate.nextIndex[me] is %v, rf.log is %v", rf.leaderstate.nextIndex[me], rf.state.log)
			
		// * update the `rf.leaderstate.nextIndex[me]`
		rf.leaderstate.nextIndex[me]++
		// rf.mu.Unlock()

		// * construct the AppendEntriesArgs and AppendEntriesReply
		// ! construct it privately or individually
		// appendentriesargs := AppendEntriesArgs{term, me, prevLogIndex, prevLogTerm, rf.state.log[prevLogIndex + 1 : ], leaderCommit}
		// appendentriesreply := AppendEntriesReply{-1, false}

		// * send `AppendEntries` parallelly to other servers
		appendChan := make(chan bool)
		for i := 0; i < len(rf.peers); i++ {
			if i != me {
				// rf.mu.Lock()
				nextIndex := rf.leaderstate.nextIndex[rf.me]
				sendEntries := rf.state.log[prevLogIndex + 1 : ]
				// rf.mu.Unlock()
				
				// * construct `args` and `reply` for each `rf.sendAppendEntries()`
				args := &AppendEntriesArgs{term, me, prevLogIndex, prevLogTerm, sendEntries, leaderCommit}
				reply := &AppendEntriesReply{-1, false}

				go func (server int, term int, me int, prevLogIndex int, prevLogTerm int, leaderCommit int) {
				// go func (server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
					for {
						if rf.sendAppendEntries(server, args, reply) {
							if reply.Success {
								appendChan <- reply.Success
								rf.loggerPrivate.Infof("receive from server %v, reply.Term is %v, reply.Success is %v", server, reply.Term, reply.Success)
								return
							} else {
								if args.Term < reply.Term {
									appendChan <- reply.Success
									rf.loggerPrivate.Infof("receive from server %v, reply.Success is %v, args.Term is %v, reply.Term is %v", server, reply.Success, args.Term, reply.Term)
									return
								}
								// * reconstruct `AppendEntriesArgs` as logs in server don't match Leader
								nextIndex--						// * decrement index of next log entry being sent
								// rf.mu.Lock()
								prevLogIndex := rf.state.log[nextIndex - 1].Index
								prevLogTerm := rf.state.log[prevLogIndex].Term 
								me := rf.me
								term := rf.state.currentTerm
								sendEntries := rf.state.log[prevLogIndex + 1 : ]
								leaderCommit := rf.commitIndex
								// rf.mu.Unlock()
								*args = AppendEntriesArgs{term, me, prevLogIndex, prevLogTerm, sendEntries, leaderCommit}
								rf.loggerPrivate.Infof("to send server %v, reconstructed log entry is %v", server, *args)
								*reply = AppendEntriesReply{-1, false}
								}
						} else {
							// * the server maybe disconnected
							appendChan <- false
							return
						}
					}
					// if rf.sendAppendEntries(server, args, reply) {
					// 	appendChan <- reply.Success
					// } else {
					// 	appendChan <- false				// ! do not forget this, or `line 501` will wait indefinitely
					// }
				}(i, term, me, prevLogIndex, prevLogTerm, leaderCommit)
			}
		}

		// * count the number of servers that successfully append the new log
		// appendsSucc := 1
		// totalAppend := 0
		// for isAppend := range appendChan {
		// 	totalAppend++
		// 	if isAppend {
		// 		appendsSucc++
		// 	}
		// 	rf.loggerPrivate.Infof("totalAppend is %v, len(rf.peers) is %v", totalAppend, len(rf.peers)) 		// todo: check if it's dead cycle
		// 	if totalAppend == (len(rf.peers) - 1) {
		// 		close(appendChan)
		// 	}
		// }
		// if appendsSucc > (len(rf.peers) / 2) {
		// 	rf.mu.Lock()
		// 	rf.commitIndex++									// * included in `AppendEntriesArgs`, make follower know which log entry should commit
		// 	index = rf.state.log[lastElemIndex].Index + 1		// * the new log entry's index should be 1 greater than `LastElemIndex`
		// 	rf.loggerPrivate.Infof("the index for return is %v, rf.state.log is %v", index, rf.state.log)
		// 	rf.mu.Unlock()

		// 	// ! whenever a commend commit, remember send a `ApplyMsg` to `rf.applyCh`
		// 	applymsg := ApplyMsg{true, command, index}
		// 	// * send the committed log entry to `rf.applyCh`
		// 	rf.applyCh <- applymsg
		// 	rf.mu.Lock()
		// 	rf.loggerPrivate.Infof("have sent applymsg %v, rf.commitIndex is %v, rf.isLeader is %v", applymsg, rf.commitIndex, rf.state.isLeader)
		// 	rf.mu.Unlock()
		// } else {
		// 	index = -1
		// }

		// ! better mechanism handling successful `rf.sendAppendEntries()`
		appendsSucc := 1
		isCommitted := false				// * prevent repeat commition
		go func (appendsSucc int, lastElemIndex int, isCommitted *bool) {
			totalAppend := 0
			for isAppend := range appendChan {
				totalAppend++
				if isAppend {
					appendsSucc++
					if (appendsSucc > (len(rf.peers) / 2)) && (!(*isCommitted)) {
						// * append successfully and update the relavent data
						rf.mu.Lock()
						rf.commitIndex++
						index := rf.state.log[lastElemIndex].Index + 1
						rf.loggerPrivate.Infof("rf.commitIndex is %v, index is %v", rf.commitIndex, index)

						// * send the entrty to `rf.applyCh`
						applymsg := ApplyMsg{true, rf.state.log[rf.commitIndex].Command, rf.state.log[rf.commitIndex].Index}
						rf.applyCh <- applymsg

						rf.loggerPrivate.Infof("have sent applymsg %v, rf.commitIndex is %v, rf.isLeader is %v", applymsg, rf.commitIndex, rf.state.isLeader)
						rf.mu.Unlock()

						*isCommitted = true
					} else {
						index = -1
					}
				}
				if totalAppend == (len(rf.peers) - 1) {
					close(appendChan)
					return
				}
			}
		}(appendsSucc, lastElemIndex, &isCommitted)
	rf.loggerPrivate.Infof("-----------------PART 2B (%v) ENDS------------------", command)
	}

	rf.mu.Unlock()
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
	rf.state.log = []LogEntry{{0, nil, rf.state.currentTerm}}

	// * for part 2B
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.leaderstate.nextIndex = make([]int, len(rf.peers))
	rf.leaderstate.matchIndex = make([]int, len(rf.peers))
	
	// * initialize the `rf.leaderstate`
	lastElemIndex := len(rf.state.log) - 1
	rf.leaderstate.nextIndex[me] = lastElemIndex + 1
	rf.leaderstate.matchIndex[me] = 0

	rf.loggerPrivate.Info("\n\n---------------------------------------------Start a new server init-------------------------------------------")

	// Your initialization code here (2A, 2B, 2C).
	// * leader election
	rf.state.isLeader = false
	rf.electionChan = make(chan ElectionChan)
	go rf.LeaderElection(me)
	go rf.Conver2Leader(me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

// * return true means the server is elected to be leader
func (rf *Raft) LeaderElection(me int) bool {
	times := 0
	for {
		// todo: might can use RWMutex
		// * read the value of `rf.state.isLeader`
		rf.mu.Lock()
		
		// if oldIsLeader != isLeader {
			// 	rf.loggerPrivate.Infof("oldIsLeader is %v, isLeader is %v", oldIsLeader, isLeader)
			// }
		if !rf.state.isLeader {
			interval := time.Now().Sub(rf.state.lastTimeFromLeader).Milliseconds()
			isElecTimeOut := interval > int64(rf.state.electionTimeOut)
			currTerm := rf.state.currentTerm
			if isElecTimeOut {
				times++
				rf.loggerPrivate.Infof("==============round %v of LeaderElection STARTS==============", times)
				str := "the election timeout is %v\n" + 
						"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tthe distance between now and last time from leader is %vms\n" + 
						"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.state.isleader is %v\n" + 
						"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\trf.state.currTerm is %v\n" + 
						"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tleader election's me is %v\n" + 
						"\t\t\t\t\t\t\t\t\t\t\t\t\t\t\tbegin to elect"
				// rf.logger.Infof(str, rf.state.electionTimeOut, interval, rf.state.isLeader, currTerm, me)
				rf.loggerPrivate.Infof(str, rf.state.electionTimeOut, interval, rf.state.isLeader, currTerm, me)

				rf.Convert2Candidate(me)
				rf.loggerPrivate.Infof("==============round %v of LeaderElection ENDS==============", times)
			}
		}
		rf.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
	}
}

// * return true means the server is elected to be leader
func (rf *Raft) Convert2Candidate(me int) bool {
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
	// rf.logger.Infof("rf.currenTerm is %v, rf.lastLogIndex is %v, rf.lastLogTerm is %v, me is %v", 
							// rf.state.currentTerm, rf.state.log[lastElemIndex].Index, rf.state.log[lastElemIndex].Term, me)
	// rf.loggerPrivate.Infof("rf.currenTerm is %v, rf.lastLogIndex is %v, rf.lastLogTerm is %v, me is %v", 
							// rf.state.currentTerm, rf.state.log[lastElemIndex].Index, rf.state.log[lastElemIndex].Term, me)
	requestvoteargs := RequestVoteArgs{rf.state.currentTerm, me, rf.state.log[lastElemIndex].Index, rf.state.log[lastElemIndex].Term}
	requestvotereply := RequestVoteReply{}
	// rf.mu.Unlock()
	votes := 1
	
	// ! should send request vote parallelly
	voteChan := make(chan bool)										// * used to notify whether the followers votes
	// * send RequestVote RPCs to all other servers
	for i := 0; i < len(rf.peers); i++ {
		if i != me{
			go func (server int, args *RequestVoteArgs, reply *RequestVoteReply) {
				if rf.sendRequestVote(server, args, reply) {
					voteChan <- reply.VoteGranted
					// * update the rf.state.currentTerm according to the `reply.Term`
					if (!reply.VoteGranted) && (reply.Term > rf.state.currentTerm) {
						rf.loggerPrivate.Infof("reply.Term (%v) > rf.state.currentTerm (%v)", reply.Term, rf.state.currentTerm)
						rf.state.currentTerm = reply.Term
					}
				} else {
					voteChan <- false
				}
			}(i, &requestvoteargs, &requestvotereply)
		}
	}
	// ! something wrong in corner case with this mechanism dealing with vote results
	// ! process of leader election mustn't wait infinitely
	// totalVotes := 0								// * judge if all rf.sendRequestvote return
	// for isVote := range voteChan {
	// 	totalVotes++
	// 	if isVote {
	// 		votes++
	// 		if votes > (len(rf.peers) / 2) {
	// 			rf.state.isLeader = true
	// 			rf.loggerPrivate.Infof("Conver2Candidate() converts rf.isLeader to true")
	// 		}
	// 	}
	// 	// * when all rf.sendRequestVote return, close the channel
	// 	if totalVotes == (len(rf.peers) - 1) {
	// 		close(voteChan)
	// 		if votes <= (len(rf.peers) / 2) {
	// 			rf.state.isLeader = false
	// 			rf.loggerPrivate.Infof("votes is %v, Conver2Candidate() converts rf.isLeader to false", votes)
	// 		}
	// 	}
	// }
	// if votes > (len(rf.peers) / 2) {
	// 	// rf.mu.Lock()
	// 	rf.state.isLeader = true
	// 	rf.loggerPrivate.Infof("votes is %v, Conver2Candidate() converts rf.isLeader to true", votes)
	// 	// rf.mu.Unlock()			
	// } else {
	// 	// ! absent of `rf.mu.lock()`
	// 	// rf.mu.Lock()
	// 	rf.loggerPrivate.Infof("votes is %v, Conver2Candidate() converts rf.isLeader to false", votes)
	// 	rf.state.isLeader = false
	// 	// rf.mu.Unlock()
	// }

	// * a better mechanism handling the vote result
	// * `Conver2Candidate()` will return immediately
	go func (votes int) {
		totalVotes := 0
		for isVote := range voteChan {
			totalVotes++
			if isVote {
				votes++
				if votes > (len(rf.peers) / 2) {
					rf.state.isLeader = true
					rf.loggerPrivate.Infof("votes is %v, Conver2Candidate() converts rf.isLeader to true", votes)
				}
				// else {
				// 	rf.state.isLeader = false
				// 	rf.loggerPrivate.Infof("votes is %v, Conver2Candidate() converts rf.isLeader to false", votes)
				// }
			} 
			if totalVotes == (len(rf.peers) - 1) {
				close(voteChan)
				// * for follower, may not need to revise the `isLeader` to false, as its original status is false
				// if votes <= (len(rf.peers) / 2) {
				// 	rf.state.isLeader = false
				// 	rf.loggerPrivate.Infof("votes is %v, Conver2Candidate() converts rf.isLeader to false", votes)
				// }
				return
			}
		}
	}(votes)

	return rf.state.isLeader 
}


func (rf *Raft) Conver2Leader(me int) {
	times := 0
	for {
		rf.mu.Lock()
		times++
		rf.loggerPrivate.Infof("\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t-------------------round %v of Convert2Leader BEGINS!----------------", times)
		isLeader := rf.state.isLeader
		lastElemIndex := len(rf.state.log) - 1
		prevLogIndex := rf.state.log[lastElemIndex].Index
		prevLogTerm := rf.state.log[lastElemIndex].Term
		leaderCommit := rf.commitIndex
		// rf.mu.Unlock()

		// sendFaults := 0								// * record times of sending failure
		rf.loggerPrivate.Infof("rf.state.isLeader is %v, rf.currentTerm is %v", rf.state.isLeader, rf.state.currentTerm)
		if isLeader {
			appendEntriesArgs := AppendEntriesArgs{rf.state.currentTerm, me, prevLogIndex, prevLogTerm, nil, leaderCommit}
			appendEntriesReply := AppendEntriesReply{0, false}
			rf.loggerPrivate.Infof("appendEntriesArgs is %v", appendEntriesArgs)
			appendChan := make(chan bool)
			for i := 0; i < len(rf.peers); i++ {
				// ! should call `sendAppendEntries()` parallelly and return immediately, or something will be wrong if some server fails
				// if i != me {
				// 	if rf.sendAppendEntries(i, &appendEntriesArgs, &appendEntriesReply) {
				// 		if appendEntriesReply.Success {
				// 			rf.mu.Lock()
				// 			nowIsLeader := rf.state.isLeader
				// 			rf.mu.Unlock()
				// 			if isLeader != nowIsLeader{
				// 				// rf.logger.Infof("server %v come back to follower from leader", rf.me)
				// 				// rf.loggerPrivate.Infof("server %v come back to follower from leader", rf.me)
				// 			}
				// 			// rf.logger.Infof("me is %v, i is %v", me, i)
				// 		}
				// 	} else {
				// 		sendFaults++
				// 		// * fail to receive reply from majority servers
				// 		if sendFaults > (len(rf.peers) / 2) {
				// 			// rf.logger.Infof("fail to receive reply from majority servers, rf.me is %v, sendFaults is %v", me, sendFaults)
				// 			// rf.loggerPrivate.Infof("fail to receive reply from majority servers, rf.me is %v, sendFaults is %v", me, sendFaults)
				// 			rf.mu.Lock()
				// 			rf.state.isLeader = false
				// 			rf.mu.Unlock()
				// 		}
				// 	}

				// }
				if i != me {
					go func (server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
						if rf.sendAppendEntries(server, args, reply) {
							rf.loggerPrivate.Infof("receive from server %v, reply.Success is %v", server, reply.Success)
							appendChan <- reply.Success
							if (!reply.Success) && (reply.Term > rf.state.currentTerm) {
								rf.loggerPrivate.Infof("reply.Term (%v) > rf.state.currentTerm (%v)", reply.Term, rf.state.currentTerm)
								rf.state.currentTerm = reply.Term
								rf.state.isLeader = false
							}
						} else {
							rf.loggerPrivate.Infof("rf.sendAppendEntries() failed")
							appendChan <- false 
						}
					}(i, &appendEntriesArgs, &appendEntriesReply)
				}
			}
			appendsSucc := 1

			// * two methods to handle the case some server disconnect
			// * maybe it should have make the disconnected server dump
			// ! no! it must return immediately
			// totalAppends := 0
			// for append := range appendChan {
			// 	totalAppends++
			// 	if append {
			// 		appendsSucc++
			// 		if appendsSucc <= (len(rf.peers) / 2) {
			// 			rf.loggerPrivate.Infof("appendsSucc is %v, Conver2Leader() converts the isLeader to false", appendsSucc)
			// 			rf.state.isLeader = false
			// 		} else {
			// 			rf.loggerPrivate.Infof("appendsSucc is %v, Conver2Leader() converts the isLeader to true", appendsSucc)
			// 			rf.state.isLeader = true
			// 		}
			// 	}
			// 	if totalAppends == len(rf.peers) - 1 {
			// 		rf.loggerPrivate.Infof("totalAppends is %v, len(rf.peers) - 1 is %v", totalAppends, len(rf.peers) - 1)
			// 		close(appendChan)
			// 		if appendsSucc <= (len(rf.peers) / 2) {
			// 			rf.state.isLeader = false
			// 			rf.loggerPrivate.Infof("appendsSucc is %v, rf.Conver2Leader() converts rf.isLeader to %v", appendsSucc, rf.state.isLeader)
			// 		}
			// 	}
			// }

			go func (appendsSucc int) {
				totalAppends := 0
				for append := range appendChan {
					totalAppends++
					if append {
						appendsSucc++
						if appendsSucc > (len(rf.peers) / 2) {
							rf.state.isLeader = true
						} else {
							rf.state.isLeader = false
						}
					}	
					if totalAppends == (len(rf.peers) - 1) {
						close(appendChan)
						// ! when the server disconnect, judge if successful times are greater than majority of servers
						if appendsSucc <= (len(rf.peers) / 2) {
							rf.state.isLeader = false
							rf.loggerPrivate.Infof("appendsSucc is %v, rf.Conver2Leader() converts rf.isLeader to %v", appendsSucc, rf.state.isLeader)
						}
						return
					}
				}
			}(appendsSucc)
		}
		// * periodically send heartbeat with setting the period is 120ms
		rf.mu.Unlock()
		rf.loggerPrivate.Infof("\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t-------------------round %v of Convert2Leader ENDS!----------------", times)
		time.Sleep(120 * time.Millisecond)
		// rf.loggerPrivate.Infof("Conver2Leader() after time.Sleep")
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