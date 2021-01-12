package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

func runTask(task *Task, numTask, numMapTask int, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	numReduce := numTask - numMapTask
	if task.Kind == KindMap {
		filename := task.InputFiles[0]
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatal(err)
		}
		if err := file.Close(); err != nil {
			log.Fatal(err)
		}
		intermKeyValue := mapf(filename, string(content))
		tempFiles := make([]*os.File, 0, numReduce)
		encoders := make([]*json.Encoder, 0, numReduce)
		for i := 0; i < numReduce; i++ {
			dir, err := os.Getwd()
			if err != nil {
				log.Fatal(err)
			}
			tempFile, err := ioutil.TempFile(dir, "tmp-map-output-")
			if err != nil {
				log.Fatal(err)
			}
			tempFiles = append(tempFiles, tempFile)
			encoders = append(encoders, json.NewEncoder(tempFile))
		}
		for _, kv := range intermKeyValue {
			reduceId := ihash(kv.Key) % numReduce
			if err := encoders[reduceId].Encode(&kv); err != nil {
				log.Fatal(err)
			}
		}
		for i, file := range tempFiles {
			filename := file.Name()
			if err := file.Close(); err != nil {
				log.Fatal(err)
			}
			if err = os.Rename(filename, fmt.Sprintf("mr-%v-%v", task.Id, i)); err != nil {
				log.Fatal(err)
			}
		}
	} else {
		intermKeyValue := make([]KeyValue, 0, numMapTask)
		intermFiles := make([]*os.File, 0, numMapTask)
		decoders := make([]*json.Decoder, 0, numMapTask)
		for _, filename := range task.InputFiles {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatal(err)
			}
			intermFiles = append(intermFiles, file)
			decoders = append(decoders, json.NewDecoder(file))
		}
		for _, decoder := range decoders {
			for decoder.More() {
				var kv KeyValue
				if err := decoder.Decode(&kv); err != nil {
					log.Fatal(err)
				}
				intermKeyValue = append(intermKeyValue, kv)
			}
		}
		for _, file := range intermFiles {
			if err := file.Close(); err != nil {
				log.Fatal(err)
			}
		}
		sort.Sort(ByKey(intermKeyValue))
		outputFile, err := os.Create(fmt.Sprintf("mr-out-%v", task.Id-numMapTask))
		if err != nil {
			log.Fatal(err)
		}
		i := 0
		for i < len(intermKeyValue) {
			j := i + 1
			for j < len(intermKeyValue) && intermKeyValue[j].Key == intermKeyValue[i].Key {
				j++
			}
			var values []string
			for k := i; k < j; k++ {
				values = append(values, intermKeyValue[k].Value)
			}
			output := reducef(intermKeyValue[i].Key, values)

			if _, err := fmt.Fprintf(outputFile, "%v %v\n", intermKeyValue[i].Key, output); err != nil {
				log.Fatal(err)
			}
			i = j
		}
		if err := outputFile.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	lastTaskId := -1
	for {
		var reply DispatchReply
		if ok := call("Master.DispatchTask", lastTaskId, &reply); ok {
			// fmt.Println("worker receives reply")
			switch reply.Status {
			case Assigned:
				runTask(&reply.Task, reply.NumTask, reply.NumMapTask, mapf, reducef)
				lastTaskId = reply.Task.Id
			case Pending:
				time.Sleep(5 * time.Second)
			case JobDone:
				return
			}
		}
	}

}

//
// example function to show how to make an RPC call to the master.
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
	sockname := masterSock()
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
