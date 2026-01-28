package mr

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"sync"
)

type mapTasks struct {
	available  []string
	inProgress []string
	done       []string
}
type reduceTasks struct {
	available  []uint
	inProgress []uint
	done       []uint
}

type Coordinator struct {
	// Your definitions here.
	mu          sync.Mutex
	mapTasks    mapTasks
	reduceTasks reduceTasks
	nReduce     uint
	workers     []string
}

func (c *Coordinator) RegisterWorker(args *RegisterWorkerArgs, reply *RegisterWorkerReply) error {
	log.Printf("RegisterWorker started\n")

	workerName := fmt.Sprintf("worker-id-%d", rand.Int32())
	defer log.Printf("RegisterWorker ended for worker %s.\n", workerName)

	c.workers = append(c.workers, workerName)

	reply.WorkerName = workerName
	reply.NReduce = c.nReduce
	return nil
}

func (c *Coordinator) GetMapInput(args *GetMapArgs, reply *GetMapReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("GetMapInput started for worker %s\n", args.WorkerName)
	defer log.Printf("GetMapInput ended for worker %s. Coordinator status: %v\n", args.WorkerName, c)

	mapTasks := &(c.mapTasks)

	if len(mapTasks.available) == 0 {
		if len(mapTasks.inProgress) == 0 {
			log.Println("GetMapInput returning AllMapTasksEnded")
			return AllMapTasksEnded{}
		} else {
			log.Println("GetMapInput returning AllMapTasksQueued")
			return AllMapTasksQueued{}
		}
	}

	toSchedule := mapTasks.available[len(mapTasks.available)-1]
	mapTasks.inProgress = append(mapTasks.inProgress, toSchedule)

	mapTasks.available[len(mapTasks.available)-1] = ""
	mapTasks.available = mapTasks.available[:len(mapTasks.available)-1]

	reply.MapInput = toSchedule
	return nil
	//TODO: start a thread to timeout and de-schedule a task to make it reavailable after N seconds (use N = 10 here according to the guidelines)
}

func (c *Coordinator) MarkMapDone(args *MarkMapDoneArgs, reply *MarkMapDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Printf("MarkMapDone - task %s completed by worker %s\n", args.MapInput, args.WorkerName)
	defer log.Printf("MarkMapDone - marked task %s as done by worker %s. Coordinator status: %v\n", args.MapInput, args.WorkerName, c)

	mapTasks := &(c.mapTasks)
	if !slices.Contains(mapTasks.inProgress, args.MapInput) {
		log.Printf("MarkMapDone - task %s not currently marked as in progress for execution", args.MapInput)
		return NoSuchScheduledMapTask{}
	}
	mapTasks.inProgress = slices.DeleteFunc(mapTasks.inProgress, func(taskName string) bool { return taskName == args.MapInput })
	mapTasks.done = append(mapTasks.done, args.MapInput)

	reply.DoneAck = true
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.reduceTasks.available) == 0 && len(c.reduceTasks.inProgress) == 0 && len(c.mapTasks.available) == 0 && len(c.mapTasks.inProgress) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce uint) *Coordinator {
	reduceTasksSl := make([]uint, nReduce)
	for i := 0; i < int(nReduce); i++ {
		reduceTasksSl[i] = uint(i)
	}

	c := Coordinator{
		mu: sync.Mutex{},
		mapTasks: mapTasks{
			available:  files,
			inProgress: make([]string, 0),
			done:       make([]string, 0),
		},
		reduceTasks: reduceTasks{
			available:  reduceTasksSl,
			inProgress: make([]uint, 0),
			done:       make([]uint, 0),
		},
		nReduce: nReduce,
		workers: make([]string, 0),
	}
	log.Printf("Coordinator started with inputArgs %v and reduceTasks %d\n", files, nReduce)
	// Your code here.

	c.server()
	return &c
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
