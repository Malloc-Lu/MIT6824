package mr

import (
	"encoding/json"
	"fmt"
	"strings"

	// "hash"
	"hash/fnv"
	// "io/fs"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueSlice []KeyValue

func (a KeyValueSlice) Len() int 			{return len(a)}
func (a KeyValueSlice) Swap(i, j int) 		{a[i], a[j] = a[j], a[i]}
func (a KeyValueSlice) Less(i, j int) bool 	{return a[i].Key < a[j].Key}


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	
	// WorkNm := 0
	// Your worker implementation here.
	rootpath, _ := os.Getwd()
	rootpath = rootpath + "/"

	request := Request{}
	reply := Reply{}

	call("Master.Init", &request, &reply)
	request.WorkerNm = reply.WorkNm

	for call("Master.AssignTask", &request, &reply) {
		fmt.Println(reply.Filename)
		
		var oname string
		var reduceNm int
		onameMap := make(map[string]bool)

		switch reply.Type {
		case Map:
			intermediate := []KeyValue{}
			fmt.Printf("reply.Filename is %v\n", reply.Filename)
			file, err := os.Open(rootpath + reply.Filename)
			if err != nil{
				log.Fatal(err)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil{
				log.Fatal(err)
			}
			file.Close()
			kva := mapf(reply.Filename, string(content))
			intermediate = append(intermediate, kva...)
	
			sort.Sort(KeyValueSlice(intermediate))
			i := 0
			nReduce := reply.NReduce
			for i < len(intermediate){
				j  := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				reduceNm = ihash(intermediate[i].Key) % nReduce
				// oname = fmt.Sprintf("mr-out-%v-%v.json", reply.WorkNm, reduceNm)
				oname = fmt.Sprintf("mr-out-%v.json", reduceNm)
				oname_path := rootpath + oname
				f, err := os.OpenFile(oname_path, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0755) 			// * os.O_APPEND indicate that not overwrite original file 
				if err != nil {
					log.Fatal(err)
				}
				// for k := i; k < j; k++{
				// 	fmt.Fprintf(f, "%v %v\n", intermediate[k].Key, intermediate[k].Value)
				// }
				// i = j
				// if err := f.Close(); err != nil {
				// 	log.Fatal(err)
				// }
				// * 将产生的中间键值对用json编码，方便reduce读取
				encoder := json.NewEncoder(f)
				for k := i; k < j; k++ {
					if err := encoder.Encode(intermediate[k]); err != nil {
						log.Printf("something wrong, when encode %v %v in %v", intermediate[k].Key, intermediate[k].Value, oname)
						log.Fatal(err)
					}
				}
				i = j
				if err := f.Close(); err != nil {
					log.Fatal(err)
				}
				onameMap[oname] = false
				
			}
			request.Type = Map
			request.TaskStatus.IsFinish = true
			request.TaskStatus.InterFileNameMap= onameMap
			request.RawFile = reply.Filename
			fmt.Printf("the update type is %v, the update worker is %v, the update file is %v\n",
										request.Type, request.WorkerNm, request.RawFile)
			call("Master.UpdateWorkStatus", &request, &reply)

		case Reduce:
			if reply.IsEnd {
				os.Exit(0)
			}
			intermediate := []KeyValue{}
			fmt.Printf("the worker num is %v, reply.FileName is %v\n", request.WorkerNm, reply.Filename)
			file, err := os.Open(rootpath + reply.Filename)
			if  err != nil {
				log.Fatal(err)
			}
			// * decode the intermediate json file in reply.TaskReduce
			decoder := json.NewDecoder(file)
			for {
				var kv KeyValue
				if err := decoder.Decode(&kv); err != nil {
					break
				}
				intermediate = append(intermediate, kv)
			}
			// close the intermittent file
			if err := file.Close(); err != nil {
				log.Fatal(err)
			}
			sort.Sort(KeyValueSlice(intermediate))
			i := 0
			reduceOutKV := []KeyValue{}
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				// todo: maybe i can write the output here rather than store them in a slice
				reduceOutKV = append(reduceOutKV, KeyValue{intermediate[i].Key, output})
				i = j
			}
			tmpFile, err := ioutil.TempFile(rootpath, ".temp_file_*")
			if err != nil {
				log.Fatal(err)
			}
			for _, item := range(reduceOutKV) {
				fmt.Fprintf(tmpFile, "%v %v\n", item.Key, item.Value)
			}
			if err := tmpFile.Close(); err != nil {
				log.Fatal(err)
			}
			if err := os.Rename(tmpFile.Name(), rootpath + strings.Replace(reply.Filename, ".json", ".txt", 1)); err != nil {
				log.Fatal(err)
			}
			fmt.Printf("the file %v will be removed\n", rootpath + reply.Filename)
			if err := os.Remove(rootpath + reply.Filename); err != nil {
				log.Fatal(err)
			}


			// * here needn't transmit the output of reduce worker back, just write them into a temp file,
			// * as the `sort` command can sort files directly
			// ! but need to update if the reduce worker finishes
			request.Type = Reduce
			request.TaskStatus.IsFinish = true
			request.RawFile = reply.Filename
			// // request.TaskStatus.ReduceOutSlice = reduceOutKV	
			fmt.Printf("the update type is %v, the update worker is %v, the update file is %v\n",
										request.Type, request.WorkerNm, request.RawFile)
			call("Master.UpdateWorkStatus", &request, &reply)
			// // tempFile, err := ioutil.TempFile("", "temp*.json")
			// // if err != nil {
			// // 	log.Fatal(err)
			// // }
			// * or i just don't write the output into file, but update it in the task statue
			// * create a encoder to write temp output of reduce worker
			// // encoder := json.NewEncoder(tempFile)
			// // for k := 0; k < len(reduceOutKV); k++ {
			// // 	if err := encoder.Encode(reduceOutKV[k]); err != nil {
			// // 		fmt.Printf("there is something wrong when encode the output of one reduce worker: encode %v %v in %v", 
			// // 														reduceOutKV[k].Key, reduceOutKV[k].Value, tempFile.Name())
			// // 		log.Fatal(err)
			// // 	}
			// // }
		case MapToReduce:

		}
	}

	

	
	// args := Args{1, "abcd"}
	// reply := Reply{}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := Args{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := Reply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v %v\n", reply.Y, reply.Filename)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {			// * 接口类型的参数表示可以接受任何类型的参数
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		fmt.Print("execute to worker.go:85\n")
		return true
	}

	fmt.Println(err)
	return false
}
