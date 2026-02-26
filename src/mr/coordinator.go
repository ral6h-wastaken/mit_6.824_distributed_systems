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
	"strings"
	"sync"
	"time"
)

type MapTask struct {
	name       string
	assignedTo string
	startTime  time.Time
}

type ReduceTask struct {
	id          uint
	assignedTo  string
	timeElapsed time.Time
}

type mapTasks struct {
	available  []MapTask
	inProgress []MapTask
	done       []MapTask
}

type reduceTasks struct {
	available  []ReduceTask
	inProgress []ReduceTask
	done       []ReduceTask
}

type Coordinator struct {
	// Your definitions here.
	mu                sync.Mutex
	mapOutcomeChan    chan string
	reduceOutcomeChan chan string

	mapTasks    mapTasks
	reduceTasks reduceTasks
	nReduce     uint
	workers     []string
}

// ======================================================START RPC METHODS======================================================
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

	//before assigning we check if any inProgress task have timed out (default 10 seconds), if yes we return them to the available pool
	c.checkMapTimeouts()

	mapTasks := &(c.mapTasks)

	if !slices.Contains(c.workers, args.WorkerName) {
		return fmt.Errorf("Unrecognized worker %s", args.WorkerName)
	}

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
	toSchedule.assignedTo = args.WorkerName
	toSchedule.startTime = time.Now()

	mapTasks.inProgress = append(mapTasks.inProgress, toSchedule)

	mapTasks.available[len(mapTasks.available)-1] = MapTask{}
	mapTasks.available = mapTasks.available[:len(mapTasks.available)-1]

	reply.MapInput = toSchedule.name

	log.Printf("GetMapInput ended for worker %s. Coordinator status: %s\n", args.WorkerName, c.getStatus())
	return nil
}

func (c *Coordinator) MarkMapDone(args *MarkMapDoneArgs, reply *MarkMapDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	log.Printf("MarkMapDone - task %s completed by worker %s\n", args.MapInput, args.WorkerName)

	mapTasks := &(c.mapTasks)
	hasSameName := func(task MapTask) bool {
		return task.name == args.MapInput
	}

	if !slices.ContainsFunc(mapTasks.inProgress, hasSameName) {
		log.Printf("MarkMapDone - task %s not currently marked as in progress for execution", args.MapInput)
		return NoSuchScheduledMapTask{}
	}

	mapTasks.inProgress = slices.DeleteFunc(mapTasks.inProgress, func(task MapTask) bool { return task.name == args.MapInput })
	mapTasks.done = append(mapTasks.done, MapTask{name: args.MapInput})

	reply.DoneAck = true

	log.Printf("MarkMapDone - marked task %s as done by worker %s. Coordinator status: %s\n", args.MapInput, args.WorkerName, c.getStatus())
	return nil
}

//======================================================END RPC METHODS======================================================

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
	mapTasksSl := make([]MapTask, len(files))
	for i, fileName := range files {
		mapTasksSl[i] = MapTask{name: fileName}
	}

	reduceTasksSl := make([]ReduceTask, nReduce)
	for i := 0; i < int(nReduce); i++ {
		reduceTasksSl[i] = ReduceTask{id: uint(i)}
	}

	c := Coordinator{
		mu: sync.Mutex{},
		mapTasks: mapTasks{
			available:  mapTasksSl,
			inProgress: make([]MapTask, 0),
			done:       make([]MapTask, 0),
		},
		reduceTasks: reduceTasks{
			available:  reduceTasksSl,
			inProgress: make([]ReduceTask, 0),
			done:       make([]ReduceTask, 0),
		},
		nReduce: nReduce,
		workers: make([]string, 0),
	}
	log.Printf("Coordinator started with inputArgs %v and reduceTasks %d\n", files, nReduce)
	// Your code here.

	c.server()
	return &c
}

//===============================================================================================================
//PRIVATE METHODS
//===============================================================================================================

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

// UNSAFE: this function does not lock the object state, make sure it is called from a 'synchronized' method
func (c *Coordinator) checkMapTimeouts() {
	toDeleteIndexes := make([]int, 0)

	runningMaps := c.mapTasks.inProgress
	for i, running := range runningMaps {
		if time.Since(running.startTime) >= 10*time.Second {
			log.Printf("Task %s has timed out, making it available again.\n", running.name)
			toDeleteIndexes = append(toDeleteIndexes, i)
		}
	}

	for _, index := range toDeleteIndexes {
		task := runningMaps[index]

		//TODO: think about it more carefully! how should we handle worker deaths?
		c.workers = slices.DeleteFunc(c.workers, func(worker string) bool { return worker == task.assignedTo })

		c.mapTasks.inProgress = slices.DeleteFunc(runningMaps, func(running MapTask) bool {
			return running.name == task.name
		})

		task.startTime = time.Time{}
		task.assignedTo = ""
		c.mapTasks.available = append(c.mapTasks.available, task)
	}
}

func (c *Coordinator) getStatus() any {
	var sb strings.Builder

	sb.WriteString("=== Coordinator Status ===\n")
	fmt.Fprintf(&sb, "Workers: %d\n", len(c.workers))
	fmt.Fprintf(&sb, "nReduce: %d\n\n", c.nReduce)

	// Map Tasks
	sb.WriteString("Map Tasks:\n")
	fmt.Fprintf(&sb, "  Available (%d):\n", len(c.mapTasks.available))
	for _, task := range c.mapTasks.available {
		fmt.Fprintf(&sb, "    - %s\n", task.name)
	}

	fmt.Fprintf(&sb, "  In Progress (%d):\n", len(c.mapTasks.inProgress))
	for _, task := range c.mapTasks.inProgress {
		fmt.Fprintf(&sb, "    - %s (assigned to: %s)\n", task.name, task.assignedTo)
	}

	fmt.Fprintf(&sb, "  Done (%d):\n", len(c.mapTasks.done))
	for _, task := range c.mapTasks.done {
		fmt.Fprintf(&sb, "    - %s\n", task.name)
	}

	// Reduce Tasks
	sb.WriteString("\nReduce Tasks:\n")
	fmt.Fprintf(&sb, "  Available (%d):\n", len(c.reduceTasks.available))
	for _, task := range c.reduceTasks.available {
		fmt.Fprintf(&sb, "    - Reduce #%d\n", task.id)
	}

	fmt.Fprintf(&sb, "  In Progress (%d):\n", len(c.reduceTasks.inProgress))
	for _, task := range c.reduceTasks.inProgress {
		fmt.Fprintf(&sb, "    - Reduce #%d (assigned to: %s)\n", task.id, task.assignedTo)
	}

	fmt.Fprintf(&sb, "  Done (%d):\n", len(c.reduceTasks.done))
	for _, task := range c.reduceTasks.done {
		fmt.Fprintf(&sb, "    - Reduce #%d\n", task.id)
	}

	return sb.String()
}
