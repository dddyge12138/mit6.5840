package mr

import (
	"6.5840/kvraft"
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce int
	// nMap表示有多少个输入文件, 就有多少个map任务, 当resMaps == nMap时, 发放Reduce任务
	nMap  int
	files []string
	tasks []*kvraft.Task
	// 完成了多少个map任务就+1, 当resMaps == nMap时, 任务完成, 可以分发Reduce任务
	resMaps int
	// 当resReduce == nReduce说明任务聚合完成, 可以退出Coordinator
	resReduce int

	// Worker Map
	WorkersExistMap map[string]time.Time
	mapMutex        sync.Mutex
	heartbeatCh     chan heartbeatMsg
	reportCh        chan reportMsg
	doneCh          chan struct{}
}

// 请求任务
type heartbeatMsg struct {
	response   *kvraft.HeartbeatResponse
	ok         chan struct{}
	WorkerUUID string
}

// 报告任务完成
type reportMsg struct {
	request    *kvraft.ReportRequest
	ok         chan struct{}
	WorkerUUID string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Heartbeat(request *kvraft.HeartbeatRequest, response *kvraft.HeartbeatResponse) error {
	msg := heartbeatMsg{response, make(chan struct{}), request.WorkerUUID}
	c.heartbeatCh <- msg
	<-msg.ok
	return nil
}

func (c *Coordinator) Report(request *kvraft.ReportRequest, response *kvraft.ReportResponse) error {
	msg := reportMsg{request, make(chan struct{}), request.WorkerUUID}
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
	log.Println("Coordinator开始调度")
	for {
		select {
		case msg := <-c.heartbeatCh:
			// register workerUUID
			c.mapMutex.Lock()
			c.WorkersExistMap[msg.WorkerUUID] = time.Now()
			c.mapMutex.Unlock()
			// 请求任务, 不用加锁遍历任务
			fmt.Println("worker拿到任务了")
			// 1 => 遍历任务, 查看哪些任务是可以拿去执行的
			res := msg.response
			if c.resMaps == c.nMap {
				// 说明此时任务已经完成, 可以发放reduce任务了
				fmt.Println("Map任务已全部完成, 发放Reduce任务")
				// 发放Reduce任务之前, 先检查是否完成了所有的Reduce任务
				if c.resReduce == c.nReduce {
					res.JobType = 4
					log.Println("Reduce任务完成, Notice Worker Exit")
					msg.ok <- struct{}{}
					break
				}
				flag := false
				for _, task := range c.tasks[:c.nReduce] {
					if task.Status == 0 || (task.Status == 1 && time.Now().Sub(task.StartTime) > 2 * time.Second) {
						task.StartTime = time.Now()
						task.Status = 1
						res.Job = *task
						res.JobType = 2
						flag = true
						break
					}
				}
				// 暂时没有任务可以发放, 那就先休眠
				if !flag {
					res.JobType = 3
				}
			} else {
				// 发放Map任务
				flag := false
				for _, task := range c.tasks[c.nReduce:] {
					if task.Status == 0 || (task.Status == 1 && time.Now().Sub(task.StartTime) > 2 * time.Second) {
						task.StartTime = time.Now()
						task.Status = 1
						res.Job = *task
						res.JobType = 1
						flag = true
						break
					}
				}
				// 暂时没有任务可以发放, 那就先休眠
				if !flag {
					res.JobType = 3
				}
			}
			msg.ok <- struct{}{}
		case msg := <-c.reportCh:
			// TODO 完成任务
			req := msg.request
			// register workerUUID
			c.mapMutex.Lock()
			c.WorkersExistMap[msg.WorkerUUID] = time.Now()
			c.mapMutex.Unlock()
			if !TaskPreCheck(req.Job) {
				msg.ok <- struct{}{}
				break
			}
			if req.JobType == 1 {
				// Map Task
				c.tasks[req.Job.Id].Status = 2
				c.resMaps++
			} else if req.JobType == 2 {
				// Reduce Task
				c.tasks[req.Job.Id].Status = 2
				c.resReduce++
			}

			msg.ok <- struct{}{}
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
	select {
	case <-c.doneCh:
		ret = true
	}
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
	for i := 0; i < nReduce; i++ {
		c.tasks = append(c.tasks, &kvraft.Task{
			Id:      i,
			Status:  0,
			NReduce: nReduce,
		})
	}
	for _, fname := range files {
		c.tasks = append(c.tasks, &kvraft.Task{
			Id:       len(c.tasks),
			FileName: fname,
			Status:   0,
			NReduce:  nReduce,
		})
		c.nMap++
	}
	c.WorkersExistMap = make(map[string]time.Time)
	c.heartbeatCh = make(chan heartbeatMsg)
	c.reportCh = make(chan reportMsg)
	c.doneCh = make(chan struct{})
	go c.schedule()
	go func() {
		timer := time.Tick(1 * time.Second)
		for range timer {
			c.mapMutex.Lock()
			canExist := false
			sum := len(c.WorkersExistMap)
			cnt := 0
			for _, startTime := range c.WorkersExistMap {
				if time.Now().Sub(startTime) >= 5*time.Second {
					canExist = true
					cnt++
				}
			}
			c.mapMutex.Unlock()
			if cnt != 0 && sum == cnt && canExist {
				log.Println("All Worker exist, Current Coordinator can exist")
				c.doneCh <- struct{}{}
				return
			}
		}
	}()
	c.server()
	return &c
}
