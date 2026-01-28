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
