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

const (
	TaskTypeMap     = "map"
	TaskTypeReduce  = "reduce"
	TaskTypeExpired = "expired"
)

type GetTaskRequest struct {
}

type GetTaskResponse struct {
	AllocatedTask *Task  `json:"allocated_task"`
	TaskKey       string `json:"task_key"`
	Allocated     bool   `json:"allocated"`
	NReducer      int    `json:"n_reducer"`
}

type SubmitTaskRequest struct {
	TaskToSubmit *Task  `json:"task_to_submit"`
	TaskKey      string `json:"task_key"`
	TaskType     string `json:"task_type"`
}

type SubmitTaskResponse struct {
	Success bool `json:"success"`
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
