package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type WorkerType int
const(
	Map WorkerType = iota
	Reduce
	MapToReduce					// * 来过渡
)
type Request struct{
	WorkerNm int
	TaskStatus Task
	Type WorkerType 			// * used when update the work status selecting which Task status
	RawFile string 				// * for debug
}

type Args struct {
	X int
	Str string
}

type Reply struct {
	WorkNm int
	NReduce int
	Y int
	Filename string				// * a file that as-yet-unstarted be mapped
	Type WorkerType
	// TempFile *os.File			// * for the reduce worker to write
								// ! rpc: gob error encoding body: gob: type os.File has no exported fields
	IsEnd bool
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
