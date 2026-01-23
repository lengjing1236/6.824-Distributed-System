package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := AskTaskArgs{}
		reply := AskTaskReply{}
		// Your worker implementation here.
		ok := call("Coordinator.AskTask", &args, &reply)
		if !ok {
			log.Fatal("ask task failed")
		}

		switch reply.Task_type {
		case MAP:
			doMap(mapf, reply)
		case REDUCE:
			doReduce(reducef, reply)
		case WAIT:
			time.Sleep(time.Second)
		case EXIT:
			os.Exit(0)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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

func doMap(mapf func(string, string) []KeyValue, reply AskTaskReply) {
	// 读取并用map函数处理输入文件
	Map_file_name := reply.Map_task_file
	file, err := os.Open(Map_file_name)
	if err != nil {
		log.Fatalf("cannot open %v", Map_file_name)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", Map_file_name)
	}
	file.Close()
	kva := mapf(Map_file_name, string(content))
	fmt.Println(kva) // debug

	err = writeMapOut(kva, reply) // 写出map的结果到临时的中间文件
	if err != nil {
		log.Fatal("Write map output: ", err)
	}

	// 通知coordinator，此map任务已经完成
	map_result := TaskResult{MAP, reply.Task_id}
	is_result_got := false
	ok := call("Coordinator.GetTaskResult", &map_result, &is_result_got)
	if ok {
		fmt.Printf("is map result got: %v\n", is_result_got)
	} else {
		fmt.Printf("doMap: call GetTaskResult failed\n")
	}
}

func writeMapOut(kva []KeyValue, reply AskTaskReply) (e error) {
	// 将map函数得到的结果分成NReduce个桶，写入中间文件
	// map_result := MapResult{}
	files := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)
	defer func() {
		for r, file := range files {
			file.Close()
			os.Rename(file.Name(), fmt.Sprintf("mr-%d-%d", reply.Task_id, r))
		}
	}()

	for r := 0; r < reply.NReduce; r++ {
		intermediate_file_name := fmt.Sprintf("mr-%d-%d", reply.Task_id, r)
		// 将reduce要用的中间文件名存储在map result中
		intermediate_file, err := os.CreateTemp("", intermediate_file_name+"-*")
		if err != nil {
			return err
		}
		files[r], encoders[r] = intermediate_file, json.NewEncoder(intermediate_file)
	}

	for _, kv := range kva {
		// 分桶写入key value pair
		err := encoders[ihash(kv.Key)%reply.NReduce].Encode(&kv)
		if err != nil {
			return err
		}
	}

	return nil
}

func doReduce(reducef func(string, []string) string, reply AskTaskReply) {
	// 读取对应reduce_id的一系列输入文件
	reduce_file_names := reply.Reduce_task_files
	kva := []KeyValue{}
	for _, reduce_file_name := range reduce_file_names {
		reduce_file, err := os.Open(reduce_file_name)
		if err != nil {
			log.Fatalf("cannot open %v", reduce_file_name)
		}
		dec := json.NewDecoder(reduce_file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	// 排序map的输出结果
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	// 写出reduce函数的结果
	oname := fmt.Sprintf("mr-out-%d", reply.Task_id)
	ofile, _ := os.CreateTemp("", oname+"-*")

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
	os.Rename(ofile.Name(), oname)

	// 通知coordinator 此reduce任务已经完成
	reduce_result := TaskResult{REDUCE, reply.Task_id}
	is_result_got := false
	ok := call("Coordinator.GetTaskResult", &reduce_result, &is_result_got)
	if ok {
		fmt.Printf("is reduce result got: %v\n", is_result_got)
	} else {
		fmt.Printf("doReduce: call GetTaskResult failed\n")
	}
}
