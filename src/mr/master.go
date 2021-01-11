package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskKind int

const (
	KindMap TaskKind = iota
	KindReduce
)

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type TaskInfo struct {
	Task
	Status    TaskStatus
	StartTime time.Time
}

type Master struct {
	// Your definitions here.
	mu           sync.Mutex
	numTask      int
	numMapTask   int
	numCompleted int
	tasks        []*TaskInfo
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) DispatchTask(lastTaskId int, newTask *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lastTaskId != -1 {
		task := m.tasks[lastTaskId]
		if task.Status == InProgress {
			task.Status = Completed
			m.numCompleted++
		}
	}
	if m.numCompleted < m.numMapTask {
		// There are still map tasks which are not completed.
		// First we check Idle MapTasks.
		for i := 0; i < m.numMapTask; i++ {
			mapTask := m.tasks[i]
			if mapTask.Status == Idle {
				mapTask.Status = InProgress
				mapTask.StartTime = time.Now()
				newTask = &Task{
					Id:         mapTask.Id,
					Kind:       KindMap,
					InputFiles: mapTask.InputFiles,
				}
				return nil
			}
		}
		// If there is no Idle MapTask, we check whether exists any InProgress MapTask which is timeout.
		for i := 0; i < m.numMapTask; i++ {
			mapTask := m.tasks[i]
			if mapTask.Status == InProgress {
				t := time.Now()
				if t.Sub(mapTask.StartTime).Seconds() > 10 {
					mapTask.StartTime = t
					newTask = &Task{
						Id:         mapTask.Id,
						Kind:       KindMap,
						InputFiles: mapTask.InputFiles,
					}
					return nil
				}
			}
		}
	} else {

	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.server()
	return &m
}
