package mr

import (
	"context"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskContext struct {
	msg    chan int
	worker int
}

const (
	TaskMsgFinish = iota
	TaskMsgErr
)

const (
	TaskStatusReady = iota
	TaskStatusQueue
	TaskStatusRunning
	TaskStatusFinish
	TaskStatusErr
)

const (
	MaxTaskRunTime   = time.Second * 5
	ScheduleInterval = time.Millisecond * 500
)

type Coordinator struct {
	files       []string
	nReduce     int
	phase       TaskPhase
	taskContext []*TaskContext
	wait        sync.WaitGroup
	mux         sync.Mutex
	workerSeq   int
	taskCh      chan Task //当消息队列用了
	done        bool
}

func (c *Coordinator) newTask(taskseq int) Task {
	task := Task{
		FileName: "",
		NReduce:  c.nReduce,
		NMap:     len(c.files),
		Sequence: taskseq,
		Phase:    c.phase,
		Alive:    true,
	}
	if task.Phase == MapPhase {
		task.FileName = c.files[taskseq]
	}
	return task
}

func (c *Coordinator) taskInstance(t Task, ctx context.Context) {
	select { //直到有信号量传入或超时时执行相应的操作
	case <-ctx.Done():
		c.taskCh <- t //将任务重新传入任务队列
	case msg := <-c.taskContext[t.Sequence].msg:
		switch msg {
		case TaskMsgFinish:
			c.wait.Done()
		case TaskMsgErr:
			c.taskCh <- t
		}
	}
}

//创建一个goroutine对任务状态进行监控
func (c *Coordinator) newTaskInstance(t Task) {
	ctx, cancle := context.WithTimeout(context.Background(), MaxTaskRunTime)
	go func() {
		c.taskInstance(t, ctx)
		defer cancle()
	}()
}

//初始化Map任务
func (c *Coordinator) initMapTasks() {
	c.initTasks(len(c.files))
}

//初始化Reduce任务
func (c *Coordinator) initReduceTasks() {
	c.initTasks(c.nReduce)
}

func (c *Coordinator) initTasks(tasknum int) {
	for i := 0; i < tasknum; i++ {
		c.taskContext[i] = new(TaskContext)
		c.taskCh <- c.newTask(i)
		c.taskContext[i].msg = make(chan int)
	}
	c.wait = sync.WaitGroup{}
	c.wait.Add(tasknum)
	log.Println("init tasks complete...", tasknum)
	go c.waitDone()
}

//当一个阶段的所有任务完成后执行下一步
func (c *Coordinator) waitDone() {
	c.wait.Wait()
	switch c.phase {
	case MapPhase:
		c.phase = ReducePhase
		c.initReduceTasks()
	case ReducePhase:
		c.done = true
	}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) Reg(args *RegisterArgs, reply *RegisterReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	reply.WorkerID = c.workerSeq
	c.workerSeq += 1
	return nil
}

func (c *Coordinator) Task(args *TaskArgs, reply *TaskReply) error {
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.done {
		reply.Task = &Task{
			Alive: false,
		}
		return nil
	}
	task := <-c.taskCh //任务分配完了会阻塞
	reply.Task = &task
	c.taskContext[task.Sequence].worker = args.WorkerID
	c.newTaskInstance(task)
	log.Println("send task", args.WorkerID, task)
	return nil
}

func (c *Coordinator) Report(args *ReportArgs, reply *ReportReply) error {
	if args.Phase != c.phase || args.WorkerID != c.taskContext[args.Seq].worker {
		return nil
	}
	log.Println("recive report", *args)
	if args.Done {
		c.taskContext[args.Seq].msg <- TaskMsgFinish
	} else {
		c.taskContext[args.Seq].msg <- TaskMsgErr
	}
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
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		phase:   MapPhase,
		files:   files,
		nReduce: nReduce,
	}
	// Your code here.
	if len(files) > nReduce {
		c.taskCh = make(chan Task, len(files))
		c.taskContext = make([]*TaskContext, len(files))
	} else {
		c.taskCh = make(chan Task, nReduce)
		c.taskContext = make([]*TaskContext, nReduce)
	}
	c.server()
	log.Println("coordinator is running...")
	c.initMapTasks()
	return &c
}
