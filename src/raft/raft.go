package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"log"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"io"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const Leader string = "leader"
const Candidate string = "candidate"
const Follower string = "follower"

func init() {
	log.SetOutput(io.Discard)
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 当前任期号
	currentTerm			int
	// 投票给哪个ID
	voteFor 			int
	// 自己的身份：leader，candidate(候选人), follower(跟随者)
	identity 			string
	// 是否需要开始选主, true表示需要
	needElection		bool
	// 重置channel, 提示退出当前正在进行的选主,重置为跟随者
	resetCh				chan *AppendEntriesReply
	// 结束领导的channel
	endLeadingCh		chan struct{}
	// 领导人id
	leaderId 			int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	if rf.identity == Leader {
		isleader = true
	}
	term = rf.currentTerm
	log.Printf("当前服务器%d,当前任期%d,当前身份%s,当前leaderId:%d", rf.me, rf.currentTerm,rf.identity,rf.leaderId)
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 候选人的当前任期号
	Term 				int
	// 候选人的id
	CandidateId			int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	// 接收者的当前任期号
	Term 				int
	// true表示你获得了这次投票
	VoteGranted			bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 1 => 如果Server的当前任期号大于请求中的任期号，返回false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.voteFor != -1) {
		reply.VoteGranted = false
	} else {
		reply.VoteGranted = true
		// 当前任期在心跳包接收时更新
		// rf.currentTerm = args.Term
		rf.voteFor = args.CandidateId
		// 如果投票了，就不能自己去发起选主活动
		rf.needElection = false
	}

	log.Printf("当前任期:%d,当前服务器id:%d,收到服务器%d,任期%d的投票请求,是否同意:%t", rf.currentTerm, rf.me, args.CandidateId, args.Term,reply.VoteGranted)
}

// 心跳包参数
type AppendEntriesArgs struct {
	// Leader Term
	Term 					int
	// Leader Id
	LeaderId				int
}

// 心跳包响应
type AppendEntriesReply struct {
	// Server's CurrentTerm
	Term 					int
	// true暂时表示认可, false表示不认可这次心跳
	Success 				bool
}

// 发送心跳包RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// 认可了这次心跳, 如果当前身份是candidate，那么就要转换成follower, 如果不认可，那么就直接继续选主
	var term int
	var success bool
	rf.mu.Lock()
	term = rf.currentTerm
	if term <= args.Term {
		//log.Printf("当前任期:%d,当前id:%d,收到了Leader:%d,任期%d的心跳包, 表示认可",rf.currentTerm, rf.me,args.LeaderId,args.Term)
		success = true
		if rf.identity == Candidate {
			// 修改自己的身份, 重置投票, 修改是否需要选主状态 (在resetCh里面完成)
			// 退出正在进行的选主活动
			rf.resetCh <- reply
		} else if rf.identity == Leader {
			// 旧时代的leader, 恢复成跟随者
			// 修改身份是跟随者(因为有可能自己是旧任期的leader, 那么就要修改成follower)
			rf.identity = Follower
			// 停止发送心跳给其他节点
			rf.endLeadingCh <- struct{}{}
		}
		// 修改leaderId
		rf.leaderId = args.LeaderId
		// 把任期号改成New Leader的任期号
		rf.currentTerm = args.Term
		// 取消投票
		rf.voteFor = -1
		// 禁止选主
		rf.needElection = false
	} else {
		log.Printf("当前任期:%d,当前id:%d,收到了Leader:%d,任期%d的心跳包, 不认可因为当前任期大于此请求",rf.currentTerm, rf.me,args.LeaderId,args.Term)
	}
	rf.mu.Unlock()
	reply.Success = success
	reply.Term = term
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).


	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		// 不需要开始选主过程，就把状态修改成需要开始选主, 然后继续等待一个超时时间.
		// 一旦收到心跳包就更新为false
		rf.needElection = true
		rf.mu.Unlock()
		//// 退出执行超时的选主
		//rf.resetCh <- struct{}{}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		if rf.needElection {
			// 开始选主过程
			go rf.startElection()
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.voteFor = -1
	rf.identity = Follower
	rf.resetCh = make(chan *AppendEntriesReply, 1)
	rf.endLeadingCh = make(chan struct{}, 1)
	rf.leaderId = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}

// 开始选主过程, 这是个阻塞的方法
// 如果选主失败, 当前任期号要恢复, 投票要恢复，身份要恢复，leaderId要恢复
func (rf *Raft) startElection() {
	rf.mu.Lock()
	// 1 => 当前任期号自增
	rf.currentTerm++
	// 2 => vote for itself, 投给自己
	rf.voteFor = rf.me
	// 3 => 身份转变成candidate
	rf.identity = Candidate
	// 4 => 将leaderId取消
	rf.leaderId = -1
	// 5 => SendVoteRequest for peers
	// 先解锁, 然后并发请求
	rf.mu.Unlock()
	log.Printf("当前任期: %d, 当前服务器开始选主: %d", rf.currentTerm, rf.me)
	cnt := 0
	agreeCh := make(chan int)
	for serverId := range rf.peers {
		go func(Id int) {
			if Id == rf.me {
				agreeCh <- 1
				return
			}
			args := &RequestVoteArgs{}
			reply := &RequestVoteReply{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me
			rf.sendRequestVote(Id, args, reply)
			//log.Printf("当前任期:%d, 当前服务器:%d,收到了来自%d的选票吗:%t", rf.currentTerm, rf.me, Id, reply.VoteGranted)
			if reply.VoteGranted {
				// 拿到了选票
				agreeCh <- 1
			}
		}(serverId)
	}

	// 如果超过半数同意就变成leader, 并且周期的发送心跳包给peers
	// 如果收到New Leader的心跳，就转变成follower
	for {
		select {
		case reply := <- rf.resetCh:
			// 收到new Leader的心跳包或者下一个周期来到, 那么重置回follower
			rf.resetFollower(reply)
			return
		case <- agreeCh:
			cnt++
			log.Printf("当前任期:%d, 当前服务器:%d, 当前选票数量%d", rf.currentTerm, rf.me, cnt)
			if cnt > len(rf.peers) / 2 {
				// Become New Leader
				rf.mu.Lock()
				log.Printf("当前任期: %d,当前ServerID: %d变成Leader了",rf.currentTerm, rf.me)
				rf.identity = Leader
				rf.mu.Unlock()
				// 周期的并发发送心跳包阻止重新选举
				rf.becomeNewLeader()
				// 完成此次选主，结束此次选主的流程
				return
			}
		}
	}

}

func (rf *Raft) becomeNewLeader() {
	// 周期的并发发送心跳包阻止重新选举 (间隔20ms发送一次)
	go func() {
		for {
			select {
			case <- rf.endLeadingCh:
				// 退出发送心跳包的协程
				log.Printf("当前任期:%d,当前id:%d,退出发送心跳包的协程", rf.currentTerm, rf.me)
				return
			default:
				for serverId := range rf.peers {
					go func(id int) {
						if id == rf.me {
							// 阻止自己选主就行了
							rf.mu.Lock()
							defer rf.mu.Unlock()
							rf.needElection = false
							rf.leaderId = rf.me
							return
						}
						args := &AppendEntriesArgs{}
						reply := &AppendEntriesReply{}
						args.Term = rf.currentTerm
						args.LeaderId = rf.me
						rf.sendAppendEntries(id, args, reply)
						// 如果心跳包响应的任期大于自己的任期，就更新为最新任期，并且切换成跟随者(主动下线法)
						// 修改成不主动下线, 保留之前的任期
						//rf.mu.Lock()
						//defer rf.mu.Unlock()
						//if reply.Term > rf.currentTerm {
						//	// resetToFollower
						//	// rf.resetCh <- reply
						//	// TODO Warning 这里多个协程会造成阻塞
						//	if len(rf.endLeadingCh) < cap(rf.endLeadingCh) {
						//		rf.endLeadingCh <- struct{}{}
						//	}
						//	//// 更改最新任期
						//	//rf.currentTerm = reply.Term
						//	//// 切换身份
						//	//rf.identity = Follower
						//	//// 修改投票
						//	//rf.voteFor = -1
						//}

					}(serverId)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}
	}()
}

func (rf *Raft) resetFollower(reply *AppendEntriesReply) {
	log.Printf("当前任期:%d, 当前服务器:%d, 开始重置为跟随者, 最新任期修改为%d", rf.currentTerm, rf.me, reply.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.identity = Follower
	rf.voteFor = -1
	rf.currentTerm = reply.Term
}