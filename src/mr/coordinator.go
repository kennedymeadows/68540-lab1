package mr

import (
	"log"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)


type Coordinator struct {
	// Your definitions here.
	nReduce int
	inputFiles []string

	mappedInputFiles map[string]bool
	reduceTasks map[int]bool

	mapDone bool
	reduceDone bool

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

type MapTask struct {
	Filename string
	Filenum int
	NReduce int
}

type ReduceTask struct {
	TaskNumber int
	NumFiles int
}

func (c *Coordinator) CallForTask(args *CallReply, reply *CallReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    reply.MapTask = &MapTask{}

    // Check and assign a map task if available
    for idx, file := range c.inputFiles {
		if c.mappedInputFiles[file] {
			continue
		} else {
			reply.MapTask.Filename = file
			reply.MapTask.Filenum = idx
			reply.MapTask.NReduce = c.nReduce
			c.mappedInputFiles[file] = true
			return nil
		}
    }
	c.mapDone = true
    // If all map tasks are done, start assigning reduce tasks
    reply.MapTask = nil 

    for i := 0; i < c.nReduce; i++ {
		if c.reduceTasks[i] {
			continue
		} else {
			reply.ReduceTask = &ReduceTask{}
			reply.ReduceTask.TaskNumber = i
			reply.ReduceTask.NumFiles = len(c.inputFiles)
			c.reduceTasks[i] = true
			return nil
		}
	}
	c.reduceDone = true
    reply.ReduceTask = nil
    return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := (c.mapDone && c.reduceDone)
	if ret {
		fmt.Printf("All tasks are done, shutting down\n")
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		inputFiles: files,
		mappedInputFiles: make(map[string]bool),
		reduceTasks: make(map[int]bool),
		mapDone: false,
		reduceDone: false,
		mu: sync.Mutex{},
	}
	
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = false
	}

	for _, file := range files {
		c.mappedInputFiles[file] = false
	}

	c.server()
	return &c
}
