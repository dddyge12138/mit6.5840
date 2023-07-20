package mr

import (
	"6.5840/kvraft"
	"fmt"
	"log"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce int
	files   []string
	tasks   []kvraft.Task
	// 完成了多少个map任务就+1, 当resMaps == nReduce时, 任务完成, 可以分发Reduce任务
	resMaps int
	// 当resReduce == nReduce说明任务聚合完成, 可以退出Coordinator
	resReduce int

	heartbeatCh chan heartbeatMsg
	reportCh    chan reportMsg
	doneCh      chan struct{}
}

// 请求任务
type heartbeatMsg struct {
	response *kvraft.HeartbeatResponse
	ok       chan struct{}
}

// 报告任务完成
type reportMsg struct {
	request *kvraft.ReportRequest
	ok      chan struct{}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Heartbeat(request *kvraft.HeartbeatRequest, response *kvraft.HeartbeatResponse) error {
	msg := heartbeatMsg{response, make(chan struct{})}
	c.heartbeatCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) Report(request *kvraft.ReportRequest, response *kvraft.ReportResponse) error {
	msg := reportMsg{request, make(chan struct{})}
	c.reportCh <- msg
	<-msg.ok
	return nil
}

func TaskPreCheck(job kvraft.Task) bool {
	if time.Now().Sub(job.StartTime) > 10*time.Second {
		return false
	}
	return true
}

func (c *Coordinator) schedule() {
	fmt.Println("Coordinator开始调度")
	for {
		select {
		case msg := <-c.heartbeatCh:
			// TODO 请求任务, 不用加锁遍历任务
			fmt.Println("worker拿到任务了")
			// 1 => 遍历任务, 查看哪些任务是可以拿去执行的
			res := msg.response
			if c.resMaps == c.nReduce {
				// 说明此时任务已经完成, 可以发放reduce任务了
				fmt.Println("Map任务已完成")
			} else {
				// 发放Map任务
				flag := false
				for _, task := range c.tasks {
					if task.Status == 0 || (task.Status == 1 && time.Now().Sub(task.StartTime) > 10) {
						task.StartTime = time.Now()
						res.Job = task
						res.JobType = 1
						flag = true
						break
					}
				}
				// 没有任务可以发放, 那就先休眠
				if !flag {
					res.JobType = 3
				}
			}
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			// TODO 完成任务
			req := msg.request
			if !TaskPreCheck(req.Job) {
				msg.ok <- struct{}{}
				break
			}
			if req.JobType == 1 {
				// Map Task
				c.tasks[req.Job.Id].Status = 2
				c.tasks = append(c.tasks, kvraft.Task{
					Id:       len(c.tasks),
					FileName: fmt.Sprintf("mr-*-%d", c.resMaps),
				})
				c.resMaps++
			} else if req.JobType == 2 {
				// TODO Reduce Task
			}

			msg.ok <- struct{}{}
		case <-c.doneCh:
			// 任务完成可以停止了
			break
		default:
			time.Sleep(1)
		}
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "localhost:1234")
	//sockname := coordinatorSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// TODO 初始化一些配置
	c.nReduce = nReduce
	for idx, fname := range files {
		c.tasks = append(c.tasks, kvraft.Task{
			Id:       idx,
			FileName: fname,
			Status:   0,
			NReduce:  nReduce,
		})
	}
	c.heartbeatCh = make(chan heartbeatMsg)
	c.reportCh = make(chan reportMsg)
	c.doneCh = make(chan struct{})
	go c.schedule()
	c.server()
	return &c
}
