package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
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

func ReadContentFromFile(fileName string) (string, error) {

	f, err := os.Open(fileName)
	if err != nil {
		return "", err
	}
	defer f.Close()
	contentBytes, err := ioutil.ReadAll(f)
	if err != nil {
		return "", err
	}

	return string(contentBytes), nil
}

type Pair struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

type PairByKey []*Pair

func (p PairByKey) Less(i, j int) bool {
	return strings.Compare(p[i].Key, p[j].Key) < 0
}

func (p PairByKey) Len() int {
	return len(p)
}

func (p PairByKey) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func ReadReduceFiles(taskID int) ([]*Pair, error) {
	entries, err := os.ReadDir("./")
	if err != nil {
		return nil, err
	}

	contents := []string{}
	for i := range entries {
		cur := entries[i]
		if cur.IsDir() {
			continue
		}

		match, err := regexp.MatchString(fmt.Sprintf("mr-.*-%v", taskID), cur.Name())
		if err != nil {
			return nil, err
		}
		if !match {
			continue
		}

		fmt.Printf("[ReadReduceFiles] taskID=%v, filename=%v\n", taskID, cur.Name())
		content, err := ReadContentFromFile(cur.Name())
		if err != nil {
			return nil, err
		}
		contents = append(contents, content)
	}

	allContent := strings.Join(contents, "\n")

	kvStrs := strings.Split(allContent, "\n")

	kvs := []KeyValue{}
	for _, str := range kvStrs {
		if str == "" {
			continue
		}
		kv := KeyValue{}
		err = json.Unmarshal([]byte(str), &kv)
		if err != nil {
			return nil, err
		}
		kvs = append(kvs, kv)
	}

	kvMap := make(map[string][]string)
	for i := range kvs {
		cur := kvs[i]
		if _, exist := kvMap[cur.Key]; !exist {
			kvMap[cur.Key] = []string{}
		}
		kvMap[cur.Key] = append(kvMap[cur.Key], cur.Value)
	}
	pairs := []*Pair{}
	for k, v := range kvMap {
		pairs = append(pairs, &Pair{
			Key:    k,
			Values: v,
		})
	}
	sort.Sort(PairByKey(pairs))

	return pairs, nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	fmt.Printf("[Worker] NewWorker enter...\n")
	for {
		needExist := false
		func() {
			getTaskReq := GetTaskRequest{}
			getTaskResp := GetTaskResponse{}
			ok := call("Coordinator.GetTask", &getTaskReq, &getTaskResp)
			if !ok {
				fmt.Println("[Worker] get task failed")
				return
			}
			if !getTaskResp.Allocated {
				fmt.Println("[Worker] task is not allocated")
				return
			}

			taskKey, allocatedTask := getTaskResp.TaskKey, getTaskResp.AllocatedTask
			fmt.Printf("[Worker] get task, type=%v, taskKey=%v\n", allocatedTask.TaskType, taskKey)
			switch allocatedTask.TaskType {
			case TaskTypeMap:
				content, err := ReadContentFromFile(allocatedTask.TaskName)
				if err != nil {
					fmt.Printf("[Worker] read content failed, err=%v\n", err)
				}

				resultKvs := mapf(allocatedTask.TaskName, content)
				result := make(map[int][]KeyValue)

				for i := range resultKvs {
					cur := resultKvs[i]
					idx := ihash(cur.Key) % getTaskResp.NReducer
					if _, exist := result[idx]; !exist {
						result[idx] = []KeyValue{}
					}
					result[idx] = append(result[idx], cur)
				}

				fileList := []*os.File{}
				defer func() {
					for i := range fileList {
						fileList[i].Close()
					}
				}()

				for idx, kvs := range result {
					fileName := fmt.Sprintf("mr-%v-%v", allocatedTask.TaskID, idx)
					f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
					if err != nil {
						fmt.Printf("[Worker] open file failed, fileName=%v, err=%v\n", fileName, err)
						return
					}
					fileList = append(fileList, f)

					writer := bufio.NewWriter(f)
					for i := range kvs {
						kvBytes, err := json.Marshal(kvs[i])
						if err != nil {
							fmt.Printf("[Worker] marshal kv failed, kv=%v, err=%v\n", kvs[i], err)
							return
						}

						writer.WriteString(string(kvBytes))
						writer.WriteString("\n")
						err = writer.Flush()
						if err != nil {
							fmt.Printf("[Worker] flush file failed, err=%v\n", err)
							return
						}
					}
				}
				submitTaskReq := &SubmitTaskRequest{
					TaskToSubmit: allocatedTask,
					TaskKey:      taskKey,
					TaskType:     allocatedTask.TaskType,
				}
				submitTaskResp := &SubmitTaskResponse{}
				ok = call("Coordinator.SubmitTask", &submitTaskReq, &submitTaskResp)
				if !ok {
					fmt.Println("[Worker] submit task failed")
				}

			case TaskTypeReduce:
				pairs, err := ReadReduceFiles(allocatedTask.TaskID)
				if err != nil {
					fmt.Printf("[Worker] read from reduce file failed, id=%v, err=%v", allocatedTask.TaskID, err)
				}
				fileName := fmt.Sprintf("mr-out-%v", allocatedTask.TaskID)
				f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0644)
				if err != nil {
					fmt.Printf("[Worker] open file failed, fileName=%v, err=%v\n", fileName, err)
					return
				}
				defer f.Close()
				writer := bufio.NewWriter(f)
				for i := range pairs {
					cur := pairs[i]

					value := reducef(cur.Key, cur.Values)
					writer.WriteString(fmt.Sprintf("%v %v", cur.Key, value))
					writer.WriteString("\n")
					writer.Flush()
				}
				submitTaskReq := &SubmitTaskRequest{
					TaskToSubmit: allocatedTask,
					TaskKey:      taskKey,
					TaskType:     allocatedTask.TaskType,
				}
				submitTaskResp := &SubmitTaskResponse{}
				ok = call("Coordinator.SubmitTask", &submitTaskReq, &submitTaskResp)
				if !ok {
					fmt.Println("[Worker] submit task failed")
				}
			case TaskTypeExpired:
				needExist = true
				return
			}
		}()

		if needExist {
			fmt.Println("[Worker] mr job done, exit...")
			return
		}
		// sleep一段时间
		time.Sleep(time.Second)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
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
