package main

//
// a MapReduce pseudo-application to test that workers
// execute reduce tasks in parallel.
//
// go build -buildmode=plugin rtiming.go
//

import "6.824/mr"
import "fmt"
import "os"
import "syscall"
import "time"
import "io/ioutil"

func nparallel(phase string) int {
	// create a file so that other workers will see that
	// we're running at the same time as them.
	pid := os.Getpid()
	myfilename := fmt.Sprintf("mr-worker-%s-%d", phase, pid)
	err := ioutil.WriteFile(myfilename, []byte("x"), 0666)
	if err != nil {
		panic(err)
	}

	// are any other workers running?
	// find their PIDs by scanning directory for mr-worker-XXX files.
	dd, err := os.Open(".")
	if err != nil {
		panic(err)
	}
	names, err := dd.Readdirnames(1000000)
	if err != nil {
		panic(err)
	}
	ret := 0
	for _, name := range names {
		var xpid int
		pat := fmt.Sprintf("mr-worker-%s-%%d", phase)
		n, err := fmt.Sscanf(name, pat, &xpid)
		if n == 1 && err == nil {
			err := syscall.Kill(xpid, 0)
			if err == nil {
				// if err == nil, xpid is alive.
				ret += 1
			}
		}
	}
	dd.Close()

	time.Sleep(1 * time.Second)

	err = os.Remove(myfilename)
	if err != nil {
		panic(err)
	}

	return ret
}

func Map(filename string, contents string) []mr.KeyValue {

	kva := []mr.KeyValue{}
	kva = append(kva, mr.KeyValue{"a", "1"})
	kva = append(kva, mr.KeyValue{"b", "1"})
	kva = append(kva, mr.KeyValue{"c", "1"})
	kva = append(kva, mr.KeyValue{"d", "1"})
	kva = append(kva, mr.KeyValue{"e", "1"})
	kva = append(kva, mr.KeyValue{"f", "1"})
	kva = append(kva, mr.KeyValue{"g", "1"})
	kva = append(kva, mr.KeyValue{"h", "1"})
	kva = append(kva, mr.KeyValue{"i", "1"})
	kva = append(kva, mr.KeyValue{"j", "1"})
	return kva
}

func Reduce(key string, values []string) string {
	n := nparallel("reduce")

	val := fmt.Sprintf("%d", n)

	return val
}
