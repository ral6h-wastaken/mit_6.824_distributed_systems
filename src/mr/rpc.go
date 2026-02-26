package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
// type ExampleArgs struct {
// 	X int
// }
//
// type ExampleReply struct {
// 	Y int
// }

// Add your RPC definitions here.
type RegisterWorkerArgs struct {
}

type RegisterWorkerReply struct {
	WorkerName string
	NReduce    uint
}

type GetMapArgs struct {
	WorkerName string
}

type GetMapReply struct {
	MapInput string
}

type MarkMapDoneArgs struct {
	WorkerName                 string
	MapInput                   string
	IntermediateFilesPartition map[int]string
}

type MarkMapDoneReply struct {
	DoneAck bool
}

type GetReduceArgs struct {
	WorkerName string
}

type GetReduceReply struct {
	PartitionId      uint16
	ReduceInputFiles []string
}

type MarkReduceDoneArgs struct {
	WorkerName  string
	ReduceInput uint16
	OutputFile  string
}

type MarkReduceDoneReply struct {
	DoneAck bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
