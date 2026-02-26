package mr

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

func ParseKeyValue(line string) (KeyValue, error) {
	pieces := strings.Split(line, " ")
	if len(pieces) != 2 {
		return KeyValue{}, fmt.Errorf("Invalid kb line: %s", line)
	}

	return KeyValue{
		Key:   pieces[0],
		Value: pieces[1],
	}, nil
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
	// thus we have to wrap the loop inside an anon function
	defer func() {
		for _, f := range mappedFiles {
			f.Close()
		}
	}()

	// var dbg_mf strings.Builder
	// dbg_mf.WriteString("[")
	// for i, f := range mappedFiles {
	// 	absPath, err := filepath.Abs(f.Name())
	// 	if err != nil {
	// 		panic(err)
	// 	}
	//
	// 	fmt.Fprintf(&dbg_mf, "%d: %s, ", i, absPath)
	// }
	// dbg_mf.WriteString("]")
	// log.Printf("Created files: %s\n", dbg_mf.String())
	intermediateResultFiles := make(map[int]string, nReduce)
	for i, f := range mappedFiles {
		absPath, err := filepath.Abs(f.Name())
		if err != nil {
			log.Printf("Could not get abs path, got error %s\n", err)
			panic(err)
		}

		intermediateResultFiles[i] = absPath
	}

map_loop:
	for {
		mapInputFileName, getMapErr := GetMapInput(workerId)
		//TODO: remove this artificial delay
		// time.Sleep(11 * time.Second)

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
			continue map_loop
		}
		defer mapInputFile.Close()

		fileByteContent, fileError := io.ReadAll(mapInputFile)
		if fileError != nil {
			log.Printf("Error while reading input file %s: %s", mapInputFileName, fileError)
			continue map_loop
		}

		fileStringContent := string(fileByteContent)
		// log.Printf("Read input file %s:\n%s[...]\n", mapInputFileName, fileStringContent[:10])

		kv := mapf(mapInputFileName, fileStringContent)
		// log.Printf("Computed key-value for inputfile %s: %v [truncated]\n", mapInputFileName, kv[:10])

		for _, keyVal := range kv {
			index := ihash(keyVal.Key) % int(nReduce)
			_, err := fmt.Fprintf(mappedFiles[index], "%s %s\n", keyVal.Key, keyVal.Value)
			if err != nil {
				//TODO: think about it (https://open.spotify.com/track/4SoFYjj0p7W1MRyZFF2fLk?si=2cd2b9f789a847b0)
				log.Printf("Could not write results correctly, aborting.")
				continue map_loop
			}
		}

		// Periodically, the buffered pairs are written to local
		// disk, partitioned into R ( our nreduce) regions by the partitioning
		// function. The locations of these buffered pairs on
		// the local disk are passed back to the master, who
		// is responsible for forwarding these locations to the
		// reduce workers.

		if markDoneErr := MarkMapDone(workerId, mapInputFileName, intermediateResultFiles); markDoneErr != nil {
			continue map_loop
		}

	}

reduce_loop:
	for {
		reduceInput, getReduceErr := GetReduceInput(workerId)
		//TODO: remove this artificial delay
		// time.Sleep(11 * time.Second)

		//TODO: adjust this (maybe)
		if getReduceErr != nil {
			if getReduceErr.Error() == NO_MORE_REDUCE_TASK {
				break reduce_loop
			}

			continue reduce_loop
		}

		partitionId := reduceInput.PartitionId
		inputFiles := reduceInput.ReduceInputFiles

		inputKeyVals := make([]KeyValue, len(inputFiles)) //idk about initial size
		for _, fileName := range inputFiles {
			intermediate := make([]KeyValue, 0)
			//TODO: adjust to do a remote read on the worker fs (maybe by passing thru the master)
			file, err := os.Open(fileName)
			if err != nil {
				log.Printf("Error while reading reduce file %s: %s", fileName, err)
				//TODO: ideally we should report failure to master not to hang the reduce task and re-schedule it imediately
				continue reduce_loop
			}
			defer file.Close()
			fileScanner := bufio.NewScanner(file)

			for fileScanner.Scan() {
				line := fileScanner.Text()
				kv, err := ParseKeyValue(line)
				if err != nil || kv.Key == "" {
					//TODO: we tolerate invalid lines in input file for now, should we?
					log.Printf("Error while parsing line: %s", err)
					continue
				}

				intermediate = append(intermediate, kv)
			}

			if err = fileScanner.Err(); err != nil {
				log.Printf("Got error while reading lines for file %s: %s", fileName, err)
				//TODO: here too we should report failure
				continue reduce_loop
			}

			inputKeyVals = append(inputKeyVals, intermediate...)
		}
		//
		// slices.SortFunc(inputKeyVals, func(a, b KeyValue) int {
		// 	return strings.Compare(a.Key, b.Key)	//so that we have them sorted by key
		// })

		valuesByKey := make(map[string][]string)
		for _, kv := range inputKeyVals {
			valuesByKey[kv.Key] = append(valuesByKey[kv.Key], kv.Value)
		}

		outFileName := fmt.Sprintf("mr-out-%d", partitionId)
		outFile, err := os.Create(outFileName)
		if err != nil {
			log.Printf("Error while creating output file number %d, got error %s\n", partitionId, err)
			continue reduce_loop
		}
		defer outFile.Close()

		for k, l := range valuesByKey {
			if k == "" {
				continue
			}
			fmt.Fprintf(outFile, "%s %s\n", k, reducef(k, l))
		}

		MarkReduceDone(workerId, reduceInput.PartitionId, outFileName)
	}

	log.Printf("Process finished!")

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

func MarkMapDone(workerName string, completedTask string, intFilePartitions map[int]string) error {
	args := MarkMapDoneArgs{
		WorkerName:                 workerName,
		MapInput:                   completedTask,
		IntermediateFilesPartition: intFilePartitions,
	}

	reply := MarkMapDoneReply{
		DoneAck: false,
	}

	mapDoneErr := call("Coordinator.MarkMapDone", &args, &reply)
	if mapDoneErr != nil {
		log.Printf("MarkMapDone - Got error %s\n", mapDoneErr)
		return mapDoneErr
	}

	if !reply.DoneAck {
		log.Printf("MarkMapDone - Commit not acknowledged")
		return errors.New("Commit not acknowledged")
	}

	return nil
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

func GetReduceInput(workerName string) (GetReduceReply, error) {
	args := GetReduceArgs{
		WorkerName: workerName,
	}

	reply := GetReduceReply{}
	err := call("Coordinator.GetReduceInput", &args, &reply)
	if err != nil {
		log.Printf("GetMapInput - Got error %s\n", err)
	} else {
		log.Printf("GetReduceInput - Got reduce task files %s for partition %d\n", reply.ReduceInputFiles, reply.PartitionId)
	}

	return reply, err
}

func MarkReduceDone(workerName string, completedTask uint16, outFile string) error {
	args := MarkReduceDoneArgs{
		WorkerName:  workerName,
		ReduceInput: completedTask,
		OutputFile:  outFile,
	}

	reply := MarkReduceDoneReply{
		DoneAck: false,
	}

	mapDoneErr := call("Coordinator.MarkReduceDone", &args, &reply)
	if mapDoneErr != nil {
		log.Printf("MarkReduceDone - Got error %s\n", mapDoneErr)
		return mapDoneErr
	}

	if !reply.DoneAck {
		log.Printf("MarkReduceDone - Commit not acknowledged")
		return errors.New("Commit not acknowledged")
	}

	return nil
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
