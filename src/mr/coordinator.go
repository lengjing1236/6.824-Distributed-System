package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
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

const TIME_OUT = 10 * time.Second // 超时时间，在本lab中为10秒

// 工作节点的状态信息
type WorkerStatus struct {
	WorkerID          int
	task_start        time.Time // the start time of the task which is processed now
	is_working        bool      // indicate whether the worker is processing task
	current_task_id   int       // the task id of the current executing task
	current_task_type TaskType
}

type Coordinator struct {
	// Your definitions here.
	mu                  sync.Mutex  // protect the data in the Coordinator
	Files               []string    // the filenames of all the input files
	NMap                int         // the # of map tasks
	NReduce             int         // the # of reduce tasks
	Reduce_input_fnames [][]string  // fnames[reduce_id][map_id] -> "mr-[map_id]-[reduce_id]"
	Map_states          []TaskState // record all the map tasks' status
	Reduce_states       []TaskState // record all the reduce tasks' status
	next_worker_id      int
	Workers             []WorkerStatus // maintain the workers' status
}

// Your code here -- RPC handlers for the worker to call.
// 给register的worker分配相应的身份信息
func (c *Coordinator) RegisterWorker(args *RegisterRequest, reply *WorkerIdentity) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.WorkerID = c.next_worker_id
	c.Workers = append(c.Workers, WorkerStatus{WorkerID: c.next_worker_id})
	c.next_worker_id++
	return nil
}

// the handler that respond to the worker's ask for the task
func (c *Coordinator) AskTask(args *WorkerIdentity, reply *Task) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.check_worker() // 检查超时

	for map_id := range c.NMap {
		if c.Map_states[map_id] == IDLE {
			// 填充reply，分配map任务
			reply.Task_type = MAP
			reply.Task_id = map_id
			reply.Map_task_file = c.Files[map_id]
			reply.NReduce = c.NReduce

			c.Map_states[map_id] = IN_PROCESS // 更新map task 的状态

			// 更新工作节点的状态信息
			c.Workers[args.WorkerID].is_working = true
			c.Workers[args.WorkerID].current_task_type = MAP
			c.Workers[args.WorkerID].current_task_id = map_id
			c.Workers[args.WorkerID].task_start = time.Now()

			return nil
		}
	}

	// 等待所有Map task完成
	for _, state := range c.Map_states {
		if state != COMPLETED {
			reply.Task_type = WAIT
			return nil
		}
	}

	// 分配reduce任务
	for reduce_id := range c.NReduce {
		if c.Reduce_states[reduce_id] == IDLE {
			// 填充reply相应字段，分配reduce task
			reply.Task_type = REDUCE
			reply.Task_id = reduce_id
			reply.Reduce_task_files = c.Reduce_input_fnames[reduce_id]

			c.Reduce_states[reduce_id] = IN_PROCESS // 更新map task 的状态

			// 更新worker节点状态信息
			c.Workers[args.WorkerID].is_working = true
			c.Workers[args.WorkerID].current_task_type = REDUCE
			c.Workers[args.WorkerID].current_task_id = reduce_id
			c.Workers[args.WorkerID].task_start = time.Now()

			return nil
		}
	}

	if c.UnsafeDone() {
		reply.Task_type = EXIT
	} else {
		reply.Task_type = WAIT
	}

	return nil
}

// 检查是否有worker运行超时，如果超时，需要将worker负责的task state重新设置为idle
// 此函数运行在加锁的环境下
func (c *Coordinator) check_worker() {
	for id, worker := range c.Workers {
		if worker.is_working && time.Since(worker.task_start) >= TIME_OUT {
			// 将该worker负责的task置idle
			switch worker.current_task_type {
			case MAP:
				c.Map_states[worker.current_task_id] = IDLE
			case REDUCE:
				c.Reduce_states[worker.current_task_id] = IDLE
			}
			c.Workers[id].is_working = false
			fmt.Printf("Worker %d crash\n", id)
		}
	}
}

// 拿到已完成的task的结果信息
func (c *Coordinator) GetTaskResult(args *TaskResult, reply *bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Task_type {
	case MAP:
		c.Map_states[args.Task_id] = COMPLETED
	case REDUCE:
		c.Reduce_states[args.Task_id] = COMPLETED
	}
	c.Workers[args.WorkerID].is_working = false

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
	c := Coordinator{Files: files, NMap: len(files), NReduce: nReduce, next_worker_id: 0}

	c.Reduce_input_fnames = make([][]string, nReduce)
	for r := range c.Reduce_input_fnames {
		for map_id := range files {
			c.Reduce_input_fnames[r] = append(c.Reduce_input_fnames[r], fmt.Sprintf("mr-%d-%d", map_id, r))
		}
	}

	c.Map_states = make([]TaskState, c.NMap)
	c.Reduce_states = make([]TaskState, nReduce)

	c.server()
	return &c
}
