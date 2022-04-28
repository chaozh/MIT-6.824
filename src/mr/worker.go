package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

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

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	w := worker{
		mapf:    mapf,
		reducef: reducef,
	}
	if err := w.register(); err != nil {
		return
	}
	log.Println("worker is running...")
	w.run()
}

func (w *worker) register() error {
	args := RegisterArgs{}
	reply := RegisterReply{}
	err := call("Coordinator.Reg", &args, &reply)
	if err != nil {
		return err
	}
	w.id = reply.WorkerID
	return nil
}

func (w *worker) getTask() (*Task, error) {
	args := TaskArgs{
		w.id,
	}
	reply := TaskReply{}
	err := call("Coordinator.Task", &args, &reply)
	if err != nil {
		return nil, err
	}
	log.Println("recive task", *reply.Task)
	return reply.Task, nil
}

func (w *worker) reportTask(t Task, done bool) error {
	args := ReportArgs{
		WorkerID: w.id,
		Done:     done,
		Phase:    t.Phase,
		Seq:      t.Sequence,
	}
	reply := ReportReply{}
	log.Println("report task", args)
	err := call("Coordinator.Report", &args, &reply)
	if err != nil {
		log.Println("report task fail", args)
		return err
	}
	log.Println("report task success", args)
	return nil
}

func (w *worker) run() {
	for {
		t, err := w.getTask()
		if err != nil {
			log.Println("worker can't get task,exit...")
			return
		}
		if !t.Alive {
			log.Println("coordinator not alive,exit...")
			os.Exit(0)
		}
		switch t.Phase {
		case MapPhase:
			w.doMapF(t)
		case ReducePhase:
			w.doReduceF(t)
		default:
			log.Panic("task error...", t.Phase)
		}
	}
}

func (w *worker) doMapF(t *Task) {
	contents, err := ioutil.ReadFile(t.FileName)
	if err != nil {
		return
	}
	kvs := w.mapf(t.FileName, string(contents))
	reduce := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NReduce
		reduce[idx] = append(reduce[idx], kv)
	}
	for idx, l := range reduce {
		filename := getMapName(t.Sequence, idx)
		file, err := os.Create(filename)
		if err != nil {
			log.Println("error", err)
			w.reportTask(*t, false)
			return
		}
		enc := json.NewEncoder(file)
		for _, kv := range l {
			err := enc.Encode(&kv)
			if err != nil {
				log.Println("error", err)
				w.reportTask(*t, false)
				return
			}
		}
		if err := file.Close(); err != nil {
			log.Println("error", err)
			w.reportTask(*t, false)
			return
		}
	}
	log.Println("task complete...", t)
	w.reportTask(*t, true)
}

func (w *worker) doReduceF(t *Task) {
	maps := make(map[string][]string)
	for idx := 0; idx < t.NMap; idx++ {
		filename := getMapName(idx, t.Sequence)
		file, err := os.Open(filename)
		if err != nil {
			log.Println("error", err)
			w.reportTask(*t, false)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err.Error() != "EOF" {
					log.Panic(err)
				}
				break
			}
			if _, e := maps[kv.Key]; !e {
				maps[kv.Key] = make([]string, 0)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	res := make([]string, 0)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	if err := ioutil.WriteFile(getMergeName(t.Sequence), []byte(strings.Join(res, "")), 0666); err != nil {
		log.Println("error", err)
		w.reportTask(*t, false)
		return
	}
	w.reportTask(*t, true)
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
func call(rpcname string, args interface{}, reply interface{}) error {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}
