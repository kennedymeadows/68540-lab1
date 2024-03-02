package mr

import (
	"log"
	// "fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
)


type Coordinator struct {
	// Your definitions here.
	nReduce int
	inputFiles []string
	mappedInputFiles map[string]bool
	mappedIntermediateFiles map[int]int // map[intermediate file idx] num of input files still to map to it
	reduceTasks map[int]bool
}

// Your code here -- RPC handlers for the worker to call.

type MapTask struct {
	Filename string
	Filenum int
	NReduce int
}

type ReduceTask struct {
	Filename string
}

type CallReply struct {
	MapTask *MapTask
	ReduceTask *ReduceTask
}

func (c *Coordinator) CallMap(args *CallReply, reply *CallReply) error {
	reply.MapTask = &MapTask{}
	for idx, file := range c.inputFiles {
		if !c.mappedInputFiles[file] {
			reply.MapTask.Filename = file
			reply.MapTask.Filenum = idx
			reply.MapTask.NReduce = c.nReduce
			c.mappedInputFiles[file] = true
			return nil
		}
	}
	reply.MapTask = nil
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
	ret := false
	for _, iFile := range c.mappedInputFiles {
		if !iFile {
			return ret
		}
	}
	ret = true
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	c.inputFiles = files
	c.mappedInputFiles = make(map[string]bool, len(files))
	c.mappedIntermediateFiles = make(map[int]int)
	c.reduceTasks = make(map[int]bool)

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = false
	}
	for _, file := range files {
		c.mappedInputFiles[file] = false
		reduceTask := ihash(file) % nReduce
		c.mappedIntermediateFiles[reduceTask]++ // map[intermediate file idx] num of input files still to map to it
		c.reduceTasks[reduceTask] = true // at least one input file will be mapped to this reduce task
	}
	c.server()
	return &c
}
