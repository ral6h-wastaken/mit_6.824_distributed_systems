package mr

//a worker should keep asking for map tasks if this error is encountererd
type AllMapTasksQueued struct {}
//a worker should move on to "reduce mode" once this error is encountered
type AllMapTasksEnded struct {}
//a worker should ask for another map task if this error is encountered while calling Coordinator.MarkMapDone
type NoSuchScheduledMapTask struct {}

const NO_MORE_MAP_TASK = "No more map tasks left"
const ALL_MAP_TASKS_QUEUED = "All map tasks are running, check again in some time"
const NO_SUCH_SCHEDULED_TASK = "No such scheduled task"

func (n AllMapTasksEnded) Error() string {
	return NO_MORE_MAP_TASK
}

func (n AllMapTasksQueued) Error() string {
	return ALL_MAP_TASKS_QUEUED
}

func (n NoSuchScheduledMapTask) Error() string {
	return NO_MORE_MAP_TASK
}

//a worker should keep asking for reduce tasks if this error is encountererd
type AllReduceTasksQueued struct {}
//a worker should terminate once this error is encountered
type AllReduceTasksEnded struct {}
//a worker should ask for another reduce task if this error is encountered while calling Coordinator.MarkReduceDone
type NoSuchScheduledReduceTask struct {}

const NO_MORE_REDUCE_TASK = "No more reduce tasks left"
const ALL_REDUCE_TASKS_QUEUED = "All reduce tasks are running, check again in some time"

func (n AllReduceTasksEnded) Error() string {
	return NO_MORE_REDUCE_TASK
}

func (n AllReduceTasksQueued) Error() string {
	return ALL_REDUCE_TASKS_QUEUED
}

func (n NoSuchScheduledReduceTask) Error() string {
	return NO_MORE_REDUCE_TASK
}
