package main

//
// start a diskvd server. it's a member of some replica
// group, which has other members, and it needs to know
// how to talk to the members of the shardmaster service.
// used by ../diskv/test_test.go
//
// arguments:
//   -g groupid
//   -m masterport1 -m masterport2 ...
//   -s replicaport1 -s replicaport2 ...
//   -i my-index-in-server-port-list
//   -u unreliable
//   -d directory
//   -r restart

import "time"
import "6.824/diskv"
import "os"
import "fmt"
import "strconv"
import "runtime"

func usage() {
	fmt.Printf("Usage: diskvd -g gid -m master... -s server... -i my-index -d dir\n")
	os.Exit(1)
}

func main() {
	var gid int64 = -1     // my replica group ID
	masters := []string{}  // ports of shardmasters
	replicas := []string{} // ports of servers in my replica group
	me := -1               // my index in replicas[]
	unreliable := false
	dir := "" // store persistent data here
	restart := false

	for i := 1; i+1 < len(os.Args); i += 2 {
		a0 := os.Args[i]
		a1 := os.Args[i+1]
		if a0 == "-g" {
			gid, _ = strconv.ParseInt(a1, 10, 64)
		} else if a0 == "-m" {
			masters = append(masters, a1)
		} else if a0 == "-s" {
			replicas = append(replicas, a1)
		} else if a0 == "-i" {
			me, _ = strconv.Atoi(a1)
		} else if a0 == "-u" {
			unreliable, _ = strconv.ParseBool(a1)
		} else if a0 == "-d" {
			dir = a1
		} else if a0 == "-r" {
			restart, _ = strconv.ParseBool(a1)
		} else {
			usage()
		}
	}

	if gid < 0 || me < 0 || len(masters) < 1 || me >= len(replicas) || dir == "" {
		usage()
	}

	runtime.GOMAXPROCS(4)

	srv := diskv.StartServer(gid, masters, replicas, me, dir, restart)
	srv.Setunreliable(unreliable)

	// for safety, force quit after 10 minutes.
	time.Sleep(10 * 60 * time.Second)
	mep, _ := os.FindProcess(os.Getpid())
	mep.Kill()
}
