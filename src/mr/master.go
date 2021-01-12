package mr

import (
	"fmt"
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

func (m *Master) DispatchTask(lastTaskId int, reply *DispatchReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lastTaskId != -1 {
		task := m.tasks[lastTaskId]
		if task.Status == InProgress {
			task.Status = Completed
			m.numCompleted++
		}
	}
	if m.numCompleted == m.numTask {
		reply = &DispatchReply{
			Status: JobDone,
		}
		return nil
	}
	if m.numCompleted < m.numMapTask {
		// There are still map tasks which are not completed.
		m.dispatchTask(0, m.numMapTask, reply)
	} else {
		// All map tasks are completed so we dispatch reduce tasks
		m.dispatchTask(m.numMapTask, m.numTask, reply)
	}
	return nil
}

func (m *Master) dispatchTask(startId, endId int, reply *DispatchReply) {
	// First we check idle tasks.
	for i := startId; i < endId; i++ {
		taskInfo := m.tasks[i]
		if taskInfo.Status == Idle {
			taskInfo.Status = InProgress
			taskInfo.StartTime = time.Now()
			reply = &DispatchReply{
				Status:     Assigned,
				Task:       taskInfo.Task,
				NumTask:    m.numTask,
				NumMapTask: m.numMapTask,
			}
			return
		}
	}
	// If there is no idle task, we check whether exists any in-progress task which is timeout.
	for i := startId; i < endId; i++ {
		taskInfo := m.tasks[i]
		if taskInfo.Status == InProgress {
			t := time.Now()
			if t.Sub(taskInfo.StartTime).Seconds() > 10 {
				// The in-progress task is timeout so re-dispatch the task.
				taskInfo.StartTime = t
				reply = &DispatchReply{
					Status:     Assigned,
					Task:       taskInfo.Task,
					NumTask:    m.numTask,
					NumMapTask: m.numMapTask,
				}
				return
			}
		}
	}
	reply = &DispatchReply{
		Status: Pending,
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
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.numCompleted == m.numTask
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	nMap := len(files)
	m := Master{
		numTask:    nMap + nReduce,
		numMapTask: nMap,
		tasks:      make([]*TaskInfo, nMap+nReduce),
	}

	for i := 0; i < nMap; i++ {
		m.tasks[i] = &TaskInfo{
			Task: Task{
				Id:         i,
				Kind:       KindMap,
				InputFiles: []string{files[i]},
			},
			Status: Idle,
		}
	}
	for i := nMap; i < m.numTask; i++ {
		inputFiles := make([]string, 0, nMap)
		for j := 0; j < nMap; j++ {
			inputFiles = append(inputFiles, fmt.Sprintf("mr-%v-%v", j, i-nMap))
		}
		m.tasks[i] = &TaskInfo{
			Task: Task{
				Id:         i,
				Kind:       KindReduce,
				InputFiles: inputFiles,
			},
			Status: Idle,
		}
	}

	m.server()
	return &m
}
