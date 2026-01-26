package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type TaskType int
type TaskState int

const (
	MAP TaskType = iota
	REDUCE
	WAIT
	EXIT
)

const (
	IDLE TaskState = iota
	IN_PROCESS
	COMPLETED
)

type Coordinator struct {
	// Your definitions here.
	Files               []string
	NReduce             int
	Reduce_input_fnames [][]string // fnames[reduce_id][map_id] -> "mr-[map_id]-[reduce_id]"
	Map_states          []TaskState
	Reduce_states       []TaskState
	map_task_id         int
	reduce_task_id      int
	mu                  sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// the handler that respond to the worker's ask for the task
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.map_task_id < len(c.Files) {
		// 填充reply，分配map任务
		reply.Task_type = MAP
		reply.Task_id = c.map_task_id
		reply.Map_task_file = c.Files[c.map_task_id]
		reply.NReduce = c.NReduce

		c.Map_states[c.map_task_id] = IN_PROCESS // 更新map task 的状态
		c.map_task_id++
		return nil
	}

	for _, state := range c.Map_states {
		if state != COMPLETED {
			reply.Task_type = WAIT
			return nil
		}
	}

	// 分配reduce任务
	if c.reduce_task_id < c.NReduce {
		reply.Task_type = REDUCE
		reply.Task_id = c.reduce_task_id
		reply.Reduce_task_files = c.Reduce_input_fnames[c.reduce_task_id]

		c.reduce_task_id++
		return nil
	}

	if c.UnsafeDone() {
		reply.Task_type = EXIT
	} else {
		reply.Task_type = WAIT
	}

	return nil
}

func (c *Coordinator) GetTaskResult(args *TaskResult, reply *bool) error {
	c.mu.Lock()
	switch args.Task_type {
	case MAP:
		c.Map_states[args.Task_id] = COMPLETED
	case REDUCE:
		c.Reduce_states[args.Task_id] = COMPLETED
	}
	c.mu.Unlock()

	*reply = true
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.UnsafeDone()
}

// 不线程安全的检查是否所有整个job已经完成
func (c *Coordinator) UnsafeDone() bool {
	for _, state := range c.Reduce_states {
		if state != COMPLETED {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{Files: files, NReduce: nReduce, map_task_id: 0, reduce_task_id: 0}
	// fmt.Println(files)
	c.Reduce_input_fnames = make([][]string, nReduce)
	for r := range c.Reduce_input_fnames {
		for map_id := range files {
			c.Reduce_input_fnames[r] = append(c.Reduce_input_fnames[r], fmt.Sprintf("mr-%d-%d", map_id, r))
		}
	}
	// fmt.Println(c.Reduce_input_fnames)

	c.Map_states = make([]TaskState, len(files))
	c.Reduce_states = make([]TaskState, nReduce)

	c.server()
	return &c
}
