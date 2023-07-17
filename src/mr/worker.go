package mr

import (
	"6.5840/kvraft"
	"errors"
	"fmt"
	"os"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	/**		开始索取任务 			**/
	// 1 => 轮训请求任务, 如果没有任务就睡眠, 按照任务类型执行不同的行为
	for {
		reply, err := CallTask()
		if err != nil {
			fmt.Errorf("请求服务器失败： %s", err.Error())
			os.Exit(1001)
		}
		switch reply.JobType {
		case 1:
			log.Println("我拿到Map任务啦")
			// TODO 执行Map任务
		case 2:
			log.Println("我拿到Reduce任务啦")
		case 3:
			log.Println("我没拿到任务, 睡眠5秒")
			time.Sleep(5 * time.Second)
		case 4:
			log.Println("任务都做完了, 没任务了, 那我退出")
			os.Exit(1000)
		default:
			log.Println("不认识的任务类型")
		}
		time.Sleep(1 * time.Second)
	}


	/*****************************/

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func CallTask() (*kvraft.HeartbeatResponse, error) {
	args := kvraft.HeartbeatRequest{}
	reply := kvraft.HeartbeatResponse{}
	err := call("Coordinator.Heartbeat", &args, &reply)
	fmt.Println(reply)
	if err {
		return &reply, nil
	} else {
		// 请求出错
		return nil, errors.New("请求Coordinator服务器出错")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "localhost:1234")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
