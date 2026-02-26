package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.
	workerId, nReduce, registerErr := RegisterWorker()
	if registerErr != nil {
		panic(registerErr)
	}

	mappedFiles := make([]*os.File, 0, nReduce)
	for i := 0; i < int(nReduce); i++ {
		f, err := os.Create(fmt.Sprintf("mapped-%s-%d", workerId, i))
		if err != nil {
			panic(err)
		}

		mappedFiles = append(mappedFiles, f)
	}

	// iirc defer in a loop executes right after the loop
	defer func() {
		for _, f := range mappedFiles {
			f.Close()
		}
	}()
	
	// dbg_mf := "["
	// for i, f := range mappedFiles {
	// 	dbg_mf += fmt.Sprintf("%d: %s, ", i, f.Name())
	// }
	// log.Printf("Created files: %s\n", dbg_mf)

map_loop:
	for {
		mapInputFileName, getMapErr := GetMapInput(workerId)
		time.Sleep(11 * time.Second)

		if getMapErr != nil {
			if getMapErr.Error() == ALL_MAP_TASKS_QUEUED {
				time.Sleep(5 * time.Second)
				continue map_loop
			} else if getMapErr.Error() == NO_MORE_MAP_TASK {
				break map_loop
			}
		}

		mapInputFile, fileError := os.Open(mapInputFileName)
		if fileError != nil {
			log.Printf("Error while opening input file %s: %s", mapInputFileName, fileError)
			panic(fileError)
		}
		defer mapInputFile.Close()

		fileByteContent, fileError := io.ReadAll(mapInputFile)
		if fileError != nil {
			log.Printf("Error while reading input file %s: %s", mapInputFileName, fileError)
			panic(fileError)
		}

		fileStringContent := string(fileByteContent)
		log.Printf("Read input file %s:\n%s[...]\n", mapInputFileName, fileStringContent[:10])

		kv := mapf(mapInputFileName, fileStringContent)
		log.Printf("Computed key-value for inputfile %s: %v [truncated]\n", mapInputFileName, kv[:10])

		for _, keyVal := range kv {
			index := ihash(keyVal.Key) % int(nReduce)
			_, err := fmt.Fprintf(mappedFiles[index], "%s %s\n", keyVal.Key, keyVal.Value)
			if err != nil {
				//TODO: think about it (https://open.spotify.com/track/4SoFYjj0p7W1MRyZFF2fLk?si=2cd2b9f789a847b0)
				log.Printf("Could not write results correctly, aborting.")
				panic(err)
			}
		}

		// Periodically, the buffered pairs are written to local
		// disk, partitioned into R ( our nreduce) regions by the partitioning
		// function. The locations of these buffered pairs on
		// the local disk are passed back to the master, who
		// is responsible for forwarding these locations to the
		// reduce workers.

		if markDoneErr := MarkMapDone(workerId, mapInputFileName); markDoneErr != nil {
			continue map_loop
		}

	}

reduce_loop:
	for {
		if false {
			break reduce_loop
		}
	}

}

func RegisterWorker() (string, uint, error) {
	args := RegisterWorkerArgs{}

	reply := RegisterWorkerReply{}

	err := call("Coordinator.RegisterWorker", &args, &reply)

	if err == nil {
		log.Printf("RegisterWorker - Got worker name %s and nReduce %d\n", reply.WorkerName, reply.NReduce)
		return reply.WorkerName, reply.NReduce, nil
	} else {
		log.Printf("RegisterWorker - Got error %s\n", err)
		return "", 0, err
	}
}

func MarkMapDone(workerName string, completedTask string) error {
	args := MarkMapDoneArgs{
		WorkerName: workerName,
		MapInput:   completedTask,
	}

	reply := MarkMapDoneReply{
		DoneAck: false,
	}

	mapDoneErr := call("Coordinator.MarkMapDone", &args, &reply)
	if mapDoneErr != nil {
		log.Printf("MarkMapDone - Got error %s\n", mapDoneErr)
	}
	return mapDoneErr
}

func GetMapInput(workerName string) (string, error) {
	args := GetMapArgs{
		WorkerName: workerName,
	}

	reply := GetMapReply{}
	err := call("Coordinator.GetMapInput", &args, &reply)
	if err == nil {
		log.Printf("GetMapInput - Got map task input %s\n", reply.MapInput)
		return reply.MapInput, nil
	} else {
		log.Printf("GetMapInput - Got error %s\n", err)
		return "", err
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	return c.Call(rpcname, args, reply)
}
