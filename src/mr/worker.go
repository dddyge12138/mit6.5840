package mr

import (
	"6.5840/kvraft"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func doReduce(job kvraft.Task, reducef func(string, []string) string) {
	// 1 => 读文件
	intermidiate := []KeyValue{}

	fileNames, err := filepath.Glob(fmt.Sprintf("mr-*-%d", job.Id))
	if err != nil || len(fileNames) == 0 {
		log.Fatalf("No matching files found: %s", fmt.Sprintf("mr-*-%d.txt", job.Id))
	}

	for _, fname := range fileNames {
		file, err := os.Open(fname)
		defer file.Close()
		if err != nil {
			log.Println("[DoReduce]Cannot open file %s", fname)
			continue
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermidiate = append(intermidiate, kv)
		}
	}
	// sort
	sort.Sort(ByKey(intermidiate))

	// feed
	outFileName := fmt.Sprintf("mr-out-%d", job.Id)
	ofile, _ := os.Create(outFileName)

	for i := 0; i < len(intermidiate); {
		j := i + 1
		for j < len(intermidiate) && intermidiate[i].Key == intermidiate[j].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermidiate[k].Value)
		}
		correctOutput := reducef(intermidiate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermidiate[i].Key, correctOutput)
		i = j
	}

	// report completed job
	err = ReportTask(&kvraft.ReportRequest{JobType: 2, Job: job}, &kvraft.ReportResponse{})
	if err != nil {
		log.Fatalf(err.Error())
	}
}

func doMap(job kvraft.Task, mapf func(string, string) []KeyValue) {
	file, err := os.Open(job.FileName)
	defer file.Close()
	if err != nil {
		log.Fatalf("[Domap]Cannot open file %v", job.FileName)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("[Domap]Cannot read file %v", job.FileName)
	}
	kva := mapf(job.FileName, string(content))

	// 排序kva, 遍历kva, 按键哈希后丢到文件中
	// 1 => 排序
	sort.Sort(ByKey(kva))

	// 2 => 遍历
	hm := map[int][]KeyValue{}
	for _, v := range kva {
		fileIdx := ihash(v.Key) % job.NReduce
		hm[fileIdx] = append(hm[fileIdx], v)
	}

	// 3 => 对哈希表遍历
	complete := false
	for fileIdx, vArr := range hm {
		middleFileName := fmt.Sprintf("mr-%d-%d", job.Id, fileIdx)
		f, err := os.CreateTemp("", middleFileName)
		defer func() {
			if complete {
				os.Rename(f.Name(), middleFileName)
			}
			os.Remove(f.Name())
		}()
		if err != nil {
			log.Fatalf("[Domap]创建中间文件失败, %s", err)
		}

		// 写入中间文件
		enc := json.NewEncoder(f)
		for _, v := range vArr {
			err := enc.Encode(&v)
			if err != nil {
				log.Fatalf("[Domap]写入中间文件失败, %s", err)
			}
		}
	}
	complete = true

	// 4 => 全部执行成功了, 请求RPC接口报告成功完成任务
	err = ReportTask(&kvraft.ReportRequest{
		JobType: 1,
		Job: job,
	}, &kvraft.ReportResponse{})
	if err != nil {
		log.Fatalf(err.Error())
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	/**  开始索取任务    **/
	// 1 => 轮训请求任务, 如果没有任务就睡眠, 按照任务类型执行不同的行为
	for {
		reply, err := CallTask()
		if err != nil {
			log.Fatalf("请求服务器失败： %s", err.Error())
		}
		switch reply.JobType {
		case 1:
			log.Println("我拿到Map任务啦")
			// 执行Map任务
			doMap(reply.Job, mapf)
			fmt.Println("执行完了doMap，休息20秒钟继续")
			time.Sleep(1 * time.Second)
		case 2:
			log.Println("我拿到Reduce任务啦")
			// TODO 执行Reduce任务
			doReduce(reply.Job, reducef)
			fmt.Println("执行完了doReduce, 休息20秒钟继续")
			time.Sleep(1 * time.Second)
		case 3:
			log.Println("我没拿到任务, 睡眠5秒后再次请求")
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
		return nil, errors.New("[CallTask]请求Coordinator服务器出错")
	}
}

func ReportTask(req *kvraft.ReportRequest, res *kvraft.ReportResponse) error {
	err := call("Coordinator.Report", req, res)
	if err {
		return nil
	} else {
		return errors.New("[ReportTask]请求Coordinator服务器出错")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
