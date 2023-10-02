package mr

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	TaskStatusFree = iota
	TaskStatusAllocated
	TaskStatusDone
	TaskStatusExpired
)

const (
	JobStatusNotDone = iota
	JobStatusDone
	JobStatusExit
)

const (
	StageMap = iota + 1
	StageReduce
)

type Task struct {
	TaskStatus int       `json:"task_status"`
	TaskName   string    `json:"task_name"`
	TaskUUID   string    `json:"task_uuid"` // 用于幂等性校验
	TaskID     int       `json:"task_id"`   // 作为Worker的逻辑ID
	StartTime  time.Time `json:"start_time"`
	TaskType   string    `json:"task_type"`
}

type Coordinator struct {
	// Your definitions here.
	// Map任务
	MapTasks            map[string]*Task
	MapTaskMutex        *sync.RWMutex
	MapTaskSuccessCount int32
	MapTaskCount        int32

	// Reduce任务
	ReduceTasks            map[string]*Task
	ReduceTaskMutex        *sync.RWMutex
	ReduceTaskSuccessCount int32
	ReduceTaskCount        int32

	// coordinator任务状态
	JobDone int32
	Stage   int32

	LastActivated int64
}

// Your code here -- RPC handlers for the worker to call.

func UUIDGenerator(fileName string) string {
	// todo: 先使用filename+随机数字
	return fmt.Sprintf("%v_%v", fileName, rand.Int63())
}

// GetTask .
// worker获取任务
func (c *Coordinator) GetTask(req *GetTaskRequest, resp *GetTaskResponse) error {
	atomic.StoreInt64(&c.LastActivated, time.Now().Unix())

	// nil check
	if req == nil {
		return errors.New("[GetTask] req is nil")
	}
	if resp == nil {
		return errors.New("[GetTask] resp is nil")
	}

	// 任务已结束
	if atomic.LoadInt32(&c.JobDone) == JobStatusDone {
		resp.AllocatedTask = &Task{
			TaskType: TaskTypeExpired,
		}
		resp.Allocated = true
		return nil
	}

	var taskMap map[string]*Task
	var taskMutex *sync.RWMutex
	var taskType string
	switch atomic.LoadInt32(&c.Stage) {
	case StageMap:
		taskMap, taskMutex = c.MapTasks, c.MapTaskMutex
		taskType = TaskTypeMap
	case StageReduce:
		taskMap, taskMutex = c.ReduceTasks, c.ReduceTaskMutex
		taskType = TaskTypeReduce
	default:
		panic(fmt.Sprintf("invalid stage=%v", c.Stage))
	}

	// 获取任务
	taskMutex.Lock()
	defer taskMutex.Unlock()
	for taskKey, curTask := range taskMap {
		if curTask.TaskStatus != TaskStatusFree && curTask.TaskStatus != TaskStatusExpired {
			continue
		}

		uuid := UUIDGenerator(curTask.TaskName)
		resp.AllocatedTask = &Task{
			TaskStatus: TaskStatusAllocated,
			TaskName:   curTask.TaskName,
			TaskUUID:   uuid,
			TaskID:     curTask.TaskID,
			TaskType:   taskType,
		}
		resp.Allocated = true
		resp.TaskKey = taskKey
		curTask.TaskStatus = TaskStatusAllocated
		curTask.TaskUUID = uuid
		curTask.StartTime = time.Now()
		resp.NReducer = int(c.ReduceTaskCount)
		return nil
	}

	resp.Allocated = false
	return nil
}

func (c *Coordinator) SubmitTask(req *SubmitTaskRequest, resp *SubmitTaskResponse) error {
	atomic.StoreInt64(&c.LastActivated, time.Now().Unix())

	// nil check
	if req == nil || req.TaskToSubmit == nil {
		return errors.New("[SubmitTask] req is nil")
	}
	if resp == nil {
		return errors.New("[SubmitTask] resp is nil")
	}

	var taskMap map[string]*Task
	var taskMutex *sync.RWMutex
	var allCount int32
	var curCount *int32
	switch req.TaskType {
	case TaskTypeMap:
		taskMap, taskMutex = c.MapTasks, c.MapTaskMutex
		curCount, allCount = &c.MapTaskSuccessCount, c.MapTaskCount
	case TaskTypeReduce:
		taskMap, taskMutex = c.ReduceTasks, c.ReduceTaskMutex
		curCount, allCount = &c.ReduceTaskSuccessCount, c.ReduceTaskCount

	default:
		return fmt.Errorf("[SubmitTask] invalid task type=%v", req.TaskType)
	}

	taskMutex.Lock()
	defer taskMutex.Unlock()

	task, exist := taskMap[req.TaskKey]
	if !exist {
		return fmt.Errorf("[SubmitTask] task is not exist, key=%v", req.TaskKey)
	}

	// 过期的请求
	if task.TaskStatus == TaskStatusExpired || task.TaskUUID != req.TaskToSubmit.TaskUUID {
		resp.Success = false
		return nil
	}

	cnt := atomic.AddInt32(curCount, 1)
	task.TaskStatus = TaskStatusDone

	if cnt == allCount {
		if c.Stage == StageMap {
			atomic.StoreInt32(&c.Stage, StageReduce)
		} else {
			atomic.StoreInt32(&c.JobDone, JobStatusDone)
		}
	}

	resp.Success = true
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	// Your code here.
	done := atomic.LoadInt32(&c.JobDone)
	return done == JobStatusExit
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		MapTasks:               make(map[string]*Task),
		MapTaskMutex:           &sync.RWMutex{},
		MapTaskSuccessCount:    0,
		MapTaskCount:           int32(len(files)),
		ReduceTasks:            make(map[string]*Task),
		ReduceTaskMutex:        &sync.RWMutex{},
		ReduceTaskSuccessCount: 0,
		ReduceTaskCount:        int32(nReduce),
		JobDone:                JobStatusNotDone,
		Stage:                  StageMap,
		LastActivated:          time.Now().Unix(),
	}

	for i, fileName := range files {
		c.MapTasks[fileName] = &Task{
			TaskStatus: TaskStatusFree,
			TaskName:   fileName,
			TaskUUID:   "", // 未分配
			TaskID:     i + 1,
		}
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks[strconv.Itoa(i)] = &Task{
			TaskStatus: TaskStatusFree,
			TaskName:   "",
			TaskUUID:   "",
			TaskID:     i,
		}
	}

	c.server()

	go func(c *Coordinator) {

		timer := time.NewTicker(time.Second)

		// 每秒一次
		for range timer.C {
			// 任务已结束
			if atomic.LoadInt32(&c.JobDone) == JobStatusDone {
				lastActivated := atomic.LoadInt64(&c.LastActivated)
				// 认为所有任务均已结束
				if time.Now().Unix()-lastActivated > 10 {
					atomic.StoreInt32(&c.JobDone, JobStatusExit)
					fmt.Println("All job done, Exit...")
					return
				}
				fmt.Println("All job done, wait worker exit...")
				continue
			}

			// 检查任务是否超时
			func() {
				var taskMap map[string]*Task
				var taskMutex *sync.RWMutex
				switch atomic.LoadInt32(&c.Stage) {
				case StageMap:
					taskMap, taskMutex = c.MapTasks, c.MapTaskMutex
				case StageReduce:
					taskMap, taskMutex = c.ReduceTasks, c.ReduceTaskMutex
				default:
					panic(fmt.Sprintf("invalid stage=%v", c.Stage))
				}

				// 获取任务
				taskMutex.Lock()
				defer taskMutex.Unlock()
				for _, curTask := range taskMap {
					if curTask.TaskStatus != TaskStatusAllocated {
						continue
					}
					if time.Now().Sub(curTask.StartTime) > time.Second*10 {
						curTask.TaskStatus = TaskStatusExpired
					}
				}
			}()
		}
	}(&c)

	return &c
}
