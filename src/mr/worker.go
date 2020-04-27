package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		//获取任务
		args := GetWorkArgs{}
		reply := GetWorkReply{}

		suc := call("Master.GetWork", args, &reply)
		if !suc {
			return
		}

		if len(reply.Files) > 0 {
			if reply.Type == 0 {
				doMapWork(&reply, mapf)
			} else {
				doReducerWork(&reply, reducef)
			}
		}

		if isAllDone() {
			break
		}

		time.Sleep(1 * time.Second)
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func isAllDone() bool {
	args := IsAllDoneArgs{}
	reply := IsAllDoneReply{}

	suc := call("Master.IsAllDone", args, &reply)
	if !suc {
		return true
	}

	return reply.Done
}

func doMapWork(reply *GetWorkReply, mapf func(string, string) []KeyValue) {
	file_name := reply.Files[0]
	fmt.Printf("get map work, start deal, file:%s\n", file_name)

	f, err := os.Open(file_name)
	if err != nil {
		return
	}

	defer f.Close()

	contents, err := ioutil.ReadAll(f)
	if err != nil {
		return
	}

	kvs := mapf(file_name, string(contents))

	encodes := make([]*json.Encoder, 0)

	fnames := []string{}

	for i := 0; i < reply.ReducerNum; i++ {
		tempFile, err := ioutil.TempFile("/tmp/mr", "map-*.txt")
		if err != nil {
			return
		}

		defer tempFile.Close()

		ec := json.NewEncoder(tempFile)

		encodes = append(encodes, ec)

		fnames = append(fnames, tempFile.Name())
	}

	//写入文件
	for _, v := range kvs {
		idx := ihash(v.Key) % reply.ReducerNum

		encodes[idx].Encode(&v)
	}

	//通知master完成
	dargs := WorkDoneArgs{
		Type:  0,
		Files: fnames,
		Mf:    file_name,
	}
	dreply := WorkDoneReply{}

	suc := call("Master.DoneWork", dargs, &dreply)
	if !suc {
		return
	}

	fmt.Printf("get map work, end deal, file:%s\n", file_name)
}

func doReducerWork(reply *GetWorkReply, reducef func(string, []string) string) {
	fmt.Println("get reducer work, start deal")

	intermediate := []KeyValue{}
	//从文件中读取
	for _, v := range reply.Files {
		f, err := os.Open(v)
		if err != nil {
			return
		}

		decode := json.NewDecoder(f)

		for {
			var kv KeyValue
			if err := decode.Decode(&kv); err != nil {
				break
			}

			intermediate = append(intermediate, kv)
		}

		f.Close()
	}

	sort.Sort(ByKey(intermediate))

	tempFile, err := ioutil.TempFile("/tmp/mr", "reduce-*.txt")
	if err != nil {
		return
	}

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()

	oname := fmt.Sprintf("./mr-out-%d", reply.Idx)
	os.Rename(tempFile.Name(), oname)

	//通知master完成
	dargs := WorkDoneArgs{
		Type:  1,
		Files: []string{oname},
		Idx:   reply.Idx,
	}
	dreply := WorkDoneReply{}

	suc := call("Master.DoneWork", dargs, &dreply)
	if !suc {
		return
	}

	fmt.Println("get reducer work, end deal")
}

//
// example function to show how to make an RPC call to the master.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
