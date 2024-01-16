package mr

import (
	// "fmt"
	"errors"
	"fmt"
	"io/fs"
	"io/ioutil"

	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct{
	BeginTime time.Time
	EndTime time.Time

	IsAssign bool
	IsFinish bool

	RawFileName string
	// InterFileNameSlice []string
	InterFileNameMap map[string]bool
	// ReduceOutSlice []KeyValue
}

type Master struct {
	// Your definitions here.
	// FileAssigned sync.Map			// * track raw file if be assigned
	mutex sync.Mutex
	RawFileAssigned map[string]bool		// * track the original files if be assigned
	InterFileAssigned map[string]bool	// * track the intermediate files if be assigned
	WorkerBeginTime map[int]time.Time				// * track the executed time for the worker
	TaskMap map[int]Task				// * track the worker status
	TaskReduce map[int]Task
	MapTaskNm int
	ReduceTaskNm int
	nReduce int
	isMapDone bool						// * judge if all map tasks done
	isReduceDone bool 					// * judge if all reduce tasks done
	// FinalFile *os.File
}

func test() {
	rootpath := "/home/malloc/Documents/project/github/6.824/src/mr/"
	test := ""
	if test == "" {
		fmt.Println("the test string is empty.")
	}
	if fs.ValidPath("test2.txt") {
		fmt.Println("the 'test.txt' exists")
		ofile, err := os.Open(rootpath + "test.txt")
		if err != nil {
			fmt.Println("open failed")
		}
		ofile.Close()
	} else{
		fmt.Println("it doesn't exist")
	}
	i := 0
	fn := rootpath + "test.txt"
	for i < 10 {
		f, err := os.OpenFile(fn, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0755)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Fprintf(f, "%v", i)
		i++
	}
	// * test the reture type of map[key]
	testMap := make(map[string]bool)
	testMap["123"] = false
	if _, ok := testMap["123"]; ok {
		fmt.Println("the key 123 is in testMap")
	}
	rootpath, _ = os.Getwd()
	rootpath = rootpath + "/"
	fmt.Printf("the rootpath is %v\n", rootpath)
	tmpFile, err := ioutil.TempFile(rootpath, ".tempfile_*.txt")
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		fmt.Fprintf(tmpFile, "%v\n", i)
	}
	fmt.Printf("tmpFile name is %v\n", tmpFile.Name())
	if err := tmpFile.Close(); err != nil {
		log.Fatal(err)
	}
	if err := os.Rename(tmpFile.Name(),  rootpath + "test.txt"); err != nil {
		log.Fatal(err)
	}
	t := time.Now()
	time.Sleep(10 * time.Second)
	fmt.Printf("The master slept for %vs\n", time.Now().Sub(t).Seconds())	
}
// Your code here -- RPC handlers for the worker to call.

func (m *Master) Init(request *Request, reply *Reply) error {
	// todo: maybe here need lock too
	m.mutex.Lock()
	m.MapTaskNm++
	reply.WorkNm = m.MapTaskNm
	// * give the worker a initia
	m.WorkerBeginTime[m.MapTaskNm] = time.Now()
	m.mutex.Unlock()
	return nil
}

func (m *Master) getUnprocessedFile(fileAssignedMap map[string]bool, workNum int) string {
	// todo: lock it
	m.mutex.Unlock()
	for key, value := range fileAssignedMap {
		if !value {
			fileAssignedMap[key] = true
			if !m.isMapDone {
				m.TaskMap[workNum] = Task{RawFileName: key, IsFinish: false,
											InterFileNameMap: m.TaskMap[workNum].InterFileNameMap}
			} else {
				m.TaskReduce[workNum] = Task{RawFileName: key, IsFinish: false,
												/* ReduceOutSlice: m.TaskReduce[workNum].ReduceOutSlice, */}
			}
			m.mutex.Lock()			// * because the next statement is return
			fmt.Printf("the returen key of line 122 in master.go is %v, fileAssignedMap[%v] is %v\n", key, key, fileAssignedMap[key])
			return key
		}
	}
	m.mutex.Lock()

	// * 这里换用go内置的互斥map操作
	// * 但是在修改完之后还要返回key，感觉不太方便
	// //m.FileAssigned.Range(func (key, value interface{}) bool {
	// //	if value == false {
	// //		m.FileAssigned.Store(key, true)
	// //	}
	// //})
	return ""
}

func (m *Master) waitAllMapDone() bool {
	// fmt.Printf("have accessed into waitAllMapDone(), and the `m.TaskMap`is %v\n", m.TaskMap)
	fmt.Printf("the crashed worker is %v\n", m.judgeWorkerTimeOut(m.WorkerBeginTime))
	i := 1
	now := time.Now()
	isDone := true
	m.mutex.Unlock()
	for key := range m.WorkerBeginTime {
		if m.TaskMap[key].RawFileName != "" {
			if m.TaskMap[key].IsFinish {
				m.WorkerBeginTime[key] = time.Now()
				i++
				// m.mutex.Lock()
			} else {
				// m.mutex.Lock()
				isDone = false
				// * in case the crashed worker num is not the last
				if time.Since(now).Milliseconds() > 200 {
					i++
				}
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
	m.mutex.Lock()
	fmt.Printf("the worker begin time is %v\n, the `isDone` is %v\n", m.WorkerBeginTime, isDone)
	// for i < len(m.TaskMap) + 1 {
	// 	m.mutex.Unlock()
	// 	if m.TaskMap[i].IsFinish {
	// 		m.WorkerBeginTime[i] = time.Now()
	// 		i++
	// 		m.mutex.Lock()
	// 	}else{
	// 		m.mutex.Lock()
	// 		if time.Since(now).Milliseconds() > 500 {
	// 			return false
	// 		}
	// 		time.Sleep(100 * time.Millisecond)
	// 	}
	// }
	return isDone
}

func (m *Master) waitAllReduceDone() bool {
	i := 1
	// now := time.Now()
	isDone := true
	m.mutex.Unlock()
	fmt.Printf("have accessed into waitAllMapDone(), and the `m.TaskReduce`is %v\n", m.TaskReduce)
	for key := range m.WorkerBeginTime {
		if m.TaskReduce[key].RawFileName != "" {
			if m.TaskReduce[key].IsFinish {
				m.WorkerBeginTime[key] = time.Now()
				i++
			} else {
				isDone = false
				i++
				// ! because the reduce worker will never crash
				// if time.Since(now).Milliseconds() > 200 {
					// i++
				// }
				// time.Sleep(50 * time.Millisecond)
			}
		}
	}
	m.mutex.Lock()
	// for i < len(m.TaskReduce) + 1 {
	// 	m.mutex.Unlock()
	// 	if m.TaskReduce[i].IsFinish {
	// 		i++
	// 		m.mutex.Lock()	
	// 	} else {
	// 		m.mutex.Lock()
	// 		time.Sleep(100 * time.Millisecond)
	// 	}
	// }
	return isDone
}

func (m *Master) isTimeOut(t time.Time, maxtime int) bool {
	return time.Now().Sub(t).Seconds() > float64(maxtime)
}

func (m *Master) judgeWorkerTimeOut(tasks map[int]time.Time) []int {
	// now := time.Now()
	outTimeWorkerNm := []int{}
	for key, value := range(tasks) {
		if m.isTimeOut(value, 10) {
			outTimeWorkerNm = append(outTimeWorkerNm, key)
		}
	}
	return outTimeWorkerNm
}



func (m *Master) AssignTask(request *Request, reply *Reply) error {
	reply.NReduce = m.nReduce
	// * 还有未处理的原始文件时，先完成map操作
	fmt.Printf("master.go, 173: the m.isMapDone is %v\n", m.isMapDone)
	// * record the crash workers
	m.mutex.Lock()
	// ! `m.isMapDone` also must use locker
	for !m.isMapDone {
		crashWorkerNm := m.judgeWorkerTimeOut(m.WorkerBeginTime)
		fmt.Printf("master.go, 204: the crashed worker num is %v\n", crashWorkerNm)
		for _, num := range crashWorkerNm {
			fmt.Printf("master.go, 229: the crashed num is %v, the crashed num raw file is %v\n", num, m.TaskMap[num].RawFileName)
			if m.TaskMap[num].RawFileName != "" {						// ! in case some worker haven't been assigned
				fmt.Printf("the crashed worker's rawfile is %v\n", m.TaskMap[num].RawFileName)
				// * modify the file corresponding to crash worker to false, which make it can be reassigned
				m.RawFileAssigned[m.TaskMap[num].RawFileName] = false
				delete(m.WorkerBeginTime, num)
				delete(m.TaskMap, num)									// ! or the func `m.waitAllMapDone()` will always wait
			}
		}
		reply.Filename = m.getUnprocessedFile(m.RawFileAssigned, request.WorkerNm)
		reply.Type = Map
		fmt.Printf("master.go, 215: the request.WorkerNm is %v, the request type is %v, the request file is %v\n", request.WorkerNm, request.Type, reply.Filename)
		if reply.Filename == "" {
			m.isMapDone = m.waitAllMapDone()
			// * converge the InterFileNameMap only once
			// ! there is a case: more than one worker stop here, which causes the next reduce worker get wrong
			if m.isMapDone {
				if len(m.InterFileAssigned) > 0 {
					reply.Type = MapToReduce
					m.mutex.Unlock()
					return nil
				}
				for _, value := range(m.TaskMap) {
					for k, v := range(value.InterFileNameMap) {
						m.InterFileAssigned[k] = v
					}
				}
				fmt.Printf("master.go, 185: %v\n", m.InterFileAssigned)
				reply.Type = MapToReduce
				m.mutex.Unlock()
				return nil
			}
		} else {
			m.mutex.Unlock()
			return nil
		}
	}
	// * 已经得到所有中间产生的文件，开始reduce操作
	// * first get all intermediate files
	// // for _, value := range(m.TaskMap) {
	// // 	for k, v := range(value.InterFileNameMap) {
	// // 		m.InterFileAssigned[k] = v
	// // 	}
	// // }
	// fmt.Printf("the len of m.InterFileAssigned is %v\n", len(m.InterFileAssigned))
	crashWorkerNm := m.judgeWorkerTimeOut(m.WorkerBeginTime)
	for _, num := range crashWorkerNm {
		fmt.Printf("master.go, 287: the crashed num is %v\n the `m.TaskReduce` is %v\n", num, m.TaskReduce)
		m.InterFileAssigned[m.TaskReduce[num].RawFileName] = false
		delete(m.WorkerBeginTime, num)
		delete(m.TaskReduce, num)
	}
	// * 获取未处理的中间文件
	reply.Filename = m.getUnprocessedFile(m.InterFileAssigned, request.WorkerNm)
	reply.Type = Reduce
	if reply.Filename == "" {
		m.isReduceDone = m.waitAllReduceDone()
		fmt.Printf("master.go, 306: `m.isReduceDone` is %v\n", m.isReduceDone)
		if m.isReduceDone {
			reply.IsEnd = true
		}
	} else {
		m.mutex.Unlock()
		return nil
	}
	m.mutex.Unlock()
	return errors.New("no more unprocessed raw files")
}

func (m *Master) UpdateWorkStatus(request *Request, reply *Reply) error {
	m.mutex.Lock()
	if request.Type == Map {
		tempInterFileNameMap := map[string]bool{}
		if len(m.TaskMap[request.WorkerNm].InterFileNameMap) > 0 {
			tempInterFileNameMap = m.TaskMap[request.WorkerNm].InterFileNameMap
		}
		for key, value := range(request.TaskStatus.InterFileNameMap) {
			tempInterFileNameMap[key] = value
		}
		// * temporarily only modity the `IsFinish` field
		// * it will also update the `InterFileNameMap` field
		m.TaskMap[request.WorkerNm] = Task{m.TaskMap[request.WorkerNm].BeginTime,
											m.TaskMap[request.WorkerNm].EndTime,
											m.TaskMap[request.WorkerNm].IsAssign,
											request.TaskStatus.IsFinish,
											m.TaskMap[request.WorkerNm].RawFileName,
											tempInterFileNameMap}
											/* m.TaskMap[request.WorkerNm].ReduceOutSlice} */
	}
	if request.Type == Reduce {
		// * mainly update the `IsFinish` field and `ReduceOutSlice` field
		// * append the output of one reduce worker into the slice if the slice is not empty
		// tempReduceOutSlice := []KeyValue{}
		// if len(m.TaskReduce[request.WorkerNm].ReduceOutSlice) > 0 {
			// tempReduceOutSlice = append(tempReduceOutSlice, m.TaskReduce[request.WorkerNm].ReduceOutSlice...)
		
		// tempReduceOutSlice = append(tempReduceOutSlice, request.TaskStatus.ReduceOutSlice...)
		m.TaskReduce[request.WorkerNm] = Task{m.TaskReduce[request.WorkerNm].BeginTime,
																m.TaskReduce[request.WorkerNm].EndTime,
																m.TaskReduce[request.WorkerNm].IsAssign,
																request.TaskStatus.IsFinish,
																m.TaskReduce[request.WorkerNm].RawFileName,
																m.TaskReduce[request.WorkerNm].InterFileNameMap}
																/* tempReduceOutSlice} */
	}
	m.mutex.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *Args, reply *Reply) error {
	reply.Filename = fmt.Sprintf("copy it %v", args.X)
	reply.Y = args.X + 2
	fmt.Println(reply.Y)
	fmt.Println(reply.Filename)
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)					// * go stands for starting a new goroutine
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	if m.isReduceDone {
		fmt.Printf("execute to master.go: 395\n")
		ret = true
	}
	// rootPath := "/home/malloc/Documents/project/github/6.824/src/main/"

	// Your code here.
	// if m.isReduceDone {
	// 	if err := os.Rename(m.FinalFile.Name(), rootPath + "mr-out-final.txt"); err != nil {
	// 		log.Fatal(err)
	// 	}
	// 	ret = true
	// 	if err := m.FinalFile.Close(); err != nil {
	// 		log.Fatal(err)
	// 	}
	// }


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.RawFileAssigned = make(map[string]bool)
	m.InterFileAssigned = make(map[string]bool)
	m.WorkerBeginTime = make(map[int]time.Time)
	m.TaskMap = make(map[int]Task)
	m.TaskReduce = make(map[int]Task)
	m.MapTaskNm = 0
	m.ReduceTaskNm = 0
	m.nReduce = nReduce
	m.isMapDone = false
	m.isReduceDone = false



	
	for _, file := range files{
		m.RawFileAssigned[file] = false
	}

	// Your code here.
	fmt.Println(m.RawFileAssigned)

	// test()


	m.server()
	return &m
}
