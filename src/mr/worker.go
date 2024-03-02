package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	// "time"
	"os"
	"io"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
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

func getFileContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return string(content)
}

func appendToIntermediate(intermediate map[int] []KeyValue, kva []KeyValue, nReduce int, filenum int) {
	for _, kv := range kva {
		intermediate[ihash(kv.Key) % nReduce] = append(intermediate[ihash(kv.Key) % 10], kv)
	}
	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%v-%v", filenum, i)
		ofile, _ := os.Create(oname)
		for _, kv := range intermediate[i] {
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		}
		ofile.Close()
	}
}

func reduceIntermediate(reduceTask int, numFiles int, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for i := 0; i < numFiles; i++ {
		filename := fmt.Sprintf("mr-%v-%v", i, reduceTask)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%v", reduceTask)
	ofile, _ := os.Create(oname)
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := CallArgs{}
		reply := CallReply{}
		ok := call("Coordinator.CallForTask", &args, &reply)
		if ok {
			if reply.MapTask != nil {
				fmt.Printf("##################\n")
				fmt.Printf("Filename: %v\nnReduce: %v\n", reply.MapTask.Filename, reply.MapTask.NReduce)
				fmt.Printf("##################\n")
				kva := mapf(reply.MapTask.Filename, getFileContent(reply.MapTask.Filename))
				intermediate := make(map[int] []KeyValue)
				appendToIntermediate(intermediate, kva, reply.MapTask.NReduce, reply.MapTask.Filenum)
			} else if reply.ReduceTask != nil {
				fmt.Printf("##################\n")
				fmt.Printf("Reduce task number: %v\n", reply.ReduceTask.TaskNumber)
				fmt.Printf("##################\n")
				reduceIntermediate(reply.ReduceTask.TaskNumber, reply.ReduceTask.NumFiles, mapf, reducef)
			} else {
				fmt.Printf("Did not receive any task\n")
				break
			}
		} else {
			fmt.Printf("call failed!\n")
		}
	}

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
