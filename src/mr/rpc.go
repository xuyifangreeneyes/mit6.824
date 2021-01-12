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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type Task struct {
	Id         int
	Kind       TaskKind
	InputFiles []string
}

type DispatchStatus int

const (
	Assigned DispatchStatus = iota // Master assigns a task to worker
	Pending                        // Currently there is no task to be assigned to worker but the mapreduce job is not finished.
	// For example, reduce tasks cannot be assigned when there still exists some in-progress map task.
	JobDone // The mapreduce job is done
)

type DispatchReply struct {
	Status     DispatchStatus
	Task       Task
	NumTask    int
	NumMapTask int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
