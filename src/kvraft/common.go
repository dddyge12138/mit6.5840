package kvraft

import (
	"time"
)

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type HeartbeatRequest struct {
}

type HeartbeatResponse struct {
	// 任务类型: 1 map任务。2. reduce任务。3. 睡眠等待任务。4. 退出worker进程任务
	JobType int
	Job     Task
}

type ReportRequest struct {
	JobType int
	Job     Task
}

type ReportResponse struct {
}

type Task struct {
	Id        int
	FileName  string
	StartTime time.Time
	// 任务的状态 0 表示未分配， 1 表示已分配未执行完毕, 2表示执行完成
	Status  int
	NReduce int
}
