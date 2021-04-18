package kvraft

import "6.824/porcupine"
import "6.824/models"
import "testing"
import "strconv"
import "time"
import "math/rand"
import "strings"
import "sync"
import "sync/atomic"
import "fmt"
import "io/ioutil"

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const electionTimeout = 1 * time.Second

const linearizabilityCheckTimeout = 1 * time.Second

type OpLog struct {
	operations []porcupine.Operation
	sync.Mutex
}

func (log *OpLog) Append(op porcupine.Operation) {
	log.Lock()
	defer log.Unlock()
	log.operations = append(log.operations, op)
}

func (log *OpLog) Read() []porcupine.Operation {
	log.Lock()
	defer log.Unlock()
	ops := make([]porcupine.Operation, len(log.operations))
	copy(ops, log.operations)
	return ops
}

// get/put/putappend that keep counts
func Get(cfg *config, ck *Clerk, key string, log *OpLog, cli int) string {
	start := time.Now().UnixNano()
	v := ck.Get(key)
	end := time.Now().UnixNano()
	cfg.op()
	if log != nil {
		log.Append(porcupine.Operation{
			Input:    models.KvInput{Op: 0, Key: key},
			Output:   models.KvOutput{Value: v},
			Call:     start,
			Return:   end,
			ClientId: cli,
		})
	}

	return v
}

func Put(cfg *config, ck *Clerk, key string, value string, log *OpLog, cli int) {
	start := time.Now().UnixNano()
	ck.Put(key, value)
	end := time.Now().UnixNano()
	cfg.op()
	if log != nil {
		log.Append(porcupine.Operation{
			Input:    models.KvInput{Op: 1, Key: key, Value: value},
			Output:   models.KvOutput{},
			Call:     start,
			Return:   end,
			ClientId: cli,
		})
	}
}

func Append(cfg *config, ck *Clerk, key string, value string, log *OpLog, cli int) {
	start := time.Now().UnixNano()
	ck.Append(key, value)
	end := time.Now().UnixNano()
	cfg.op()
	if log != nil {
		log.Append(porcupine.Operation{
			Input:    models.KvInput{Op: 2, Key: key, Value: value},
			Output:   models.KvOutput{},
			Call:     start,
			Return:   end,
			ClientId: cli,
		})
	}
}

func check(cfg *config, t *testing.T, ck *Clerk, key string, value string) {
	v := Get(cfg, ck, key, nil, -1)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

// a client runs the function f and then signals it is done
func run_client(t *testing.T, cfg *config, me int, ca chan bool, fn func(me int, ck *Clerk, t *testing.T)) {
	ok := false
	defer func() { ca <- ok }()
	ck := cfg.makeClient(cfg.All())
	fn(me, ck, t)
	ok = true
	cfg.deleteClient(ck)
}

// spawn ncli clients and wait until they are all done
func spawn_clients_and_wait(t *testing.T, cfg *config, ncli int, fn func(me int, ck *Clerk, t *testing.T)) {
	ca := make([]chan bool, ncli)
	for cli := 0; cli < ncli; cli++ {
		ca[cli] = make(chan bool)
		go run_client(t, cfg, cli, ca[cli], fn)
	}
	// log.Printf("spawn_clients_and_wait: waiting for clients")
	for cli := 0; cli < ncli; cli++ {
		ok := <-ca[cli]
		// log.Printf("spawn_clients_and_wait: client %d is done\n", cli)
		if ok == false {
			t.Fatalf("failure")
		}
	}
}

// predict effect of Append(k, val) if old value is prev.
func NextValue(prev string, val string) string {
	return prev + val
}

// check that for a specific client all known appends are present in a value,
// and in order
func checkClntAppends(t *testing.T, clnt int, v string, count int) {
	lastoff := -1
	for j := 0; j < count; j++ {
		wanted := "x " + strconv.Itoa(clnt) + " " + strconv.Itoa(j) + " y"
		off := strings.Index(v, wanted)
		if off < 0 {
			t.Fatalf("%v missing element %v in Append result %v", clnt, wanted, v)
		}
		off1 := strings.LastIndex(v, wanted)
		if off1 != off {
			t.Fatalf("duplicate element %v in Append result", wanted)
		}
		if off <= lastoff {
			t.Fatalf("wrong order for element %v in Append result", wanted)
		}
		lastoff = off
	}
}

// check that all known appends are present in a value,
// and are in order for each concurrent client.
func checkConcurrentAppends(t *testing.T, v string, counts []int) {
	nclients := len(counts)
	for i := 0; i < nclients; i++ {
		lastoff := -1
		for j := 0; j < counts[i]; j++ {
			wanted := "x " + strconv.Itoa(i) + " " + strconv.Itoa(j) + " y"
			off := strings.Index(v, wanted)
			if off < 0 {
				t.Fatalf("%v missing element %v in Append result %v", i, wanted, v)
			}
			off1 := strings.LastIndex(v, wanted)
			if off1 != off {
				t.Fatalf("duplicate element %v in Append result", wanted)
			}
			if off <= lastoff {
				t.Fatalf("wrong order for element %v in Append result", wanted)
			}
			lastoff = off
		}
	}
}

// repartition the servers periodically
func partitioner(t *testing.T, cfg *config, ch chan bool, done *int32) {
	defer func() { ch <- true }()
	for atomic.LoadInt32(done) == 0 {
		a := make([]int, cfg.n)
		for i := 0; i < cfg.n; i++ {
			a[i] = (rand.Int() % 2)
		}
		pa := make([][]int, 2)
		for i := 0; i < 2; i++ {
			pa[i] = make([]int, 0)
			for j := 0; j < cfg.n; j++ {
				if a[j] == i {
					pa[i] = append(pa[i], j)
				}
			}
		}
		cfg.partition(pa[0], pa[1])
		time.Sleep(electionTimeout + time.Duration(rand.Int63()%200)*time.Millisecond)
	}
}

// Basic test is as follows: one or more clients submitting Append/Get
// operations to set of servers for some period of time.  After the period is
// over, test checks that all appended values are present and in order for a
// particular key.  If unreliable is set, RPCs may fail.  If crash is set, the
// servers crash after the period is over and restart.  If partitions is set,
// the test repartitions the network concurrently with the clients and servers. If
// maxraftstate is a positive number, the size of the state for Raft (i.e., log
// size) shouldn't exceed 8*maxraftstate. If maxraftstate is negative,
// snapshots shouldn't be used.
func GenericTest(t *testing.T, part string, nclients int, nservers int, unreliable bool, crash bool, partitions bool, maxraftstate int, randomkeys bool) {

	title := "Test: "
	if unreliable {
		// the network drops RPC requests and replies.
		title = title + "unreliable net, "
	}
	if crash {
		// peers re-start, and thus persistence must work.
		title = title + "restarts, "
	}
	if partitions {
		// the network may partition
		title = title + "partitions, "
	}
	if maxraftstate != -1 {
		title = title + "snapshots, "
	}
	if randomkeys {
		title = title + "random keys, "
	}
	if nclients > 1 {
		title = title + "many clients"
	} else {
		title = title + "one client"
	}
	title = title + " (" + part + ")" // 3A or 3B

	cfg := make_config(t, nservers, unreliable, maxraftstate)
	defer cfg.cleanup()

	cfg.begin(title)
	opLog := &OpLog{}

	ck := cfg.makeClient(cfg.All())

	done_partitioner := int32(0)
	done_clients := int32(0)
	ch_partitioner := make(chan bool)
	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}
	for i := 0; i < 3; i++ {
		// log.Printf("Iteration %v\n", i)
		atomic.StoreInt32(&done_clients, 0)
		atomic.StoreInt32(&done_partitioner, 0)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
			}()
			last := "" // only used when not randomkeys
			if !randomkeys {
				Put(cfg, myck, strconv.Itoa(cli), last, opLog, cli)
			}
			for atomic.LoadInt32(&done_clients) == 0 {
				var key string
				if randomkeys {
					key = strconv.Itoa(rand.Intn(nclients))
				} else {
					key = strconv.Itoa(cli)
				}
				nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
				if (rand.Int() % 1000) < 500 {
					// log.Printf("%d: client new append %v\n", cli, nv)
					Append(cfg, myck, key, nv, opLog, cli)
					if !randomkeys {
						last = NextValue(last, nv)
					}
					j++
				} else if randomkeys && (rand.Int()%1000) < 100 {
					// we only do this when using random keys, because it would break the
					// check done after Get() operations
					Put(cfg, myck, key, nv, opLog, cli)
					j++
				} else {
					// log.Printf("%d: client new get %v\n", cli, key)
					v := Get(cfg, myck, key, opLog, cli)
					// the following check only makes sense when we're not using random keys
					if !randomkeys && v != last {
						t.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
					}
				}
			}
		})

		if partitions {
			// Allow the clients to perform some operations without interruption
			time.Sleep(1 * time.Second)
			go partitioner(t, cfg, ch_partitioner, &done_partitioner)
		}
		time.Sleep(5 * time.Second)

		atomic.StoreInt32(&done_clients, 1)     // tell clients to quit
		atomic.StoreInt32(&done_partitioner, 1) // tell partitioner to quit

		if partitions {
			// log.Printf("wait for partitioner\n")
			<-ch_partitioner
			// reconnect network and submit a request. A client may
			// have submitted a request in a minority.  That request
			// won't return until that server discovers a new term
			// has started.
			cfg.ConnectAll()
			// wait for a while so that we have a new term
			time.Sleep(electionTimeout)
		}

		if crash {
			// log.Printf("shutdown servers\n")
			for i := 0; i < nservers; i++ {
				cfg.ShutdownServer(i)
			}
			// Wait for a while for servers to shutdown, since
			// shutdown isn't a real crash and isn't instantaneous
			time.Sleep(electionTimeout)
			// log.Printf("restart servers\n")
			// crash and re-start all
			for i := 0; i < nservers; i++ {
				cfg.StartServer(i)
			}
			cfg.ConnectAll()
		}

		// log.Printf("wait for clients\n")
		for i := 0; i < nclients; i++ {
			// log.Printf("read from clients %d\n", i)
			j := <-clnts[i]
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key, opLog, 0)
			if !randomkeys {
				checkClntAppends(t, i, v, j)
			}
		}

		if maxraftstate > 0 {
			// Check maximum after the servers have processed all client
			// requests and had time to checkpoint.
			sz := cfg.LogSize()
			if sz > 8*maxraftstate {
				t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
			}
		}
		if maxraftstate < 0 {
			// Check that snapshots are not used
			ssz := cfg.SnapshotSize()
			if ssz > 0 {
				t.Fatalf("snapshot too large (%v), should not be used when maxraftstate = %d", ssz, maxraftstate)
			}
		}
	}

	res, info := porcupine.CheckOperationsVerbose(models.KvModel, opLog.Read(), linearizabilityCheckTimeout)
	if res == porcupine.Illegal {
		file, err := ioutil.TempFile("", "*.html")
		if err != nil {
			fmt.Printf("info: failed to create temp file for visualization")
		} else {
			err = porcupine.Visualize(models.KvModel, info, file)
			if err != nil {
				fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
			} else {
				fmt.Printf("info: wrote history visualization to %s\n", file.Name())
			}
		}
		t.Fatal("history is not linearizable")
	} else if res == porcupine.Unknown {
		fmt.Println("info: linearizability check timed out, assuming history is ok")
	}

	cfg.end()
}

// Check that ops are committed fast enough, better than 1 per heartbeat interval
func GenericTestSpeed(t *testing.T, part string, maxraftstate int) {
	const nservers = 3
	const numOps = 1000
	cfg := make_config(t, nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin(fmt.Sprintf("Test: ops complete fast enough (%s)", part))

	// wait until first op completes, so we know a leader is elected
	// and KV servers are ready to process client requests
	ck.Get("x")

	start := time.Now()
	for i := 0; i < numOps; i++ {
		ck.Append("x", "x 0 "+strconv.Itoa(i)+" y")
	}
	dur := time.Since(start)

	v := ck.Get("x")
	checkClntAppends(t, 0, v, numOps)

	// heartbeat interval should be ~ 100 ms; require at least 3 ops per
	const heartbeatInterval = 100 * time.Millisecond
	const opsPerInterval = 3
	const timePerOp = heartbeatInterval / opsPerInterval
	if dur > numOps*timePerOp {
		t.Fatalf("Operations completed too slowly %v/op > %v/op\n", dur/numOps, timePerOp)
	}

	cfg.end()
}

func TestBasic3A(t *testing.T) {
	// Test: one client (3A) ...
	GenericTest(t, "3A", 1, 5, false, false, false, -1, false)
}

func TestSpeed3A(t *testing.T) {
	GenericTestSpeed(t, "3A", -1)
}

func TestConcurrent3A(t *testing.T) {
	// Test: many clients (3A) ...
	GenericTest(t, "3A", 5, 5, false, false, false, -1, false)
}

func TestUnreliable3A(t *testing.T) {
	// Test: unreliable net, many clients (3A) ...
	GenericTest(t, "3A", 5, 5, true, false, false, -1, false)
}

func TestUnreliableOneKey3A(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, true, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: concurrent append to same key, unreliable (3A)")

	Put(cfg, ck, "k", "", nil, -1)

	const nclient = 5
	const upto = 10
	spawn_clients_and_wait(t, cfg, nclient, func(me int, myck *Clerk, t *testing.T) {
		n := 0
		for n < upto {
			Append(cfg, myck, "k", "x "+strconv.Itoa(me)+" "+strconv.Itoa(n)+" y", nil, -1)
			n++
		}
	})

	var counts []int
	for i := 0; i < nclient; i++ {
		counts = append(counts, upto)
	}

	vx := Get(cfg, ck, "k", nil, -1)
	checkConcurrentAppends(t, vx, counts)

	cfg.end()
}

// Submit a request in the minority partition and check that the requests
// doesn't go through until the partition heals.  The leader in the original
// network ends up in the minority partition.
func TestOnePartition3A(t *testing.T) {
	const nservers = 5
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()
	ck := cfg.makeClient(cfg.All())

	Put(cfg, ck, "1", "13", nil, -1)

	cfg.begin("Test: progress in majority (3A)")

	p1, p2 := cfg.make_partition()
	cfg.partition(p1, p2)

	ckp1 := cfg.makeClient(p1)  // connect ckp1 to p1
	ckp2a := cfg.makeClient(p2) // connect ckp2a to p2
	ckp2b := cfg.makeClient(p2) // connect ckp2b to p2

	Put(cfg, ckp1, "1", "14", nil, -1)
	check(cfg, t, ckp1, "1", "14")

	cfg.end()

	done0 := make(chan bool)
	done1 := make(chan bool)

	cfg.begin("Test: no progress in minority (3A)")
	go func() {
		Put(cfg, ckp2a, "1", "15", nil, -1)
		done0 <- true
	}()
	go func() {
		Get(cfg, ckp2b, "1", nil, -1) // different clerk in p2
		done1 <- true
	}()

	select {
	case <-done0:
		t.Fatalf("Put in minority completed")
	case <-done1:
		t.Fatalf("Get in minority completed")
	case <-time.After(time.Second):
	}

	check(cfg, t, ckp1, "1", "14")
	Put(cfg, ckp1, "1", "16", nil, -1)
	check(cfg, t, ckp1, "1", "16")

	cfg.end()

	cfg.begin("Test: completion after heal (3A)")

	cfg.ConnectAll()
	cfg.ConnectClient(ckp2a, cfg.All())
	cfg.ConnectClient(ckp2b, cfg.All())

	time.Sleep(electionTimeout)

	select {
	case <-done0:
	case <-time.After(30 * 100 * time.Millisecond):
		t.Fatalf("Put did not complete")
	}

	select {
	case <-done1:
	case <-time.After(30 * 100 * time.Millisecond):
		t.Fatalf("Get did not complete")
	default:
	}

	check(cfg, t, ck, "1", "15")

	cfg.end()
}

func TestManyPartitionsOneClient3A(t *testing.T) {
	// Test: partitions, one client (3A) ...
	GenericTest(t, "3A", 1, 5, false, false, true, -1, false)
}

func TestManyPartitionsManyClients3A(t *testing.T) {
	// Test: partitions, many clients (3A) ...
	GenericTest(t, "3A", 5, 5, false, false, true, -1, false)
}

func TestPersistOneClient3A(t *testing.T) {
	// Test: restarts, one client (3A) ...
	GenericTest(t, "3A", 1, 5, false, true, false, -1, false)
}

func TestPersistConcurrent3A(t *testing.T) {
	// Test: restarts, many clients (3A) ...
	GenericTest(t, "3A", 5, 5, false, true, false, -1, false)
}

func TestPersistConcurrentUnreliable3A(t *testing.T) {
	// Test: unreliable net, restarts, many clients (3A) ...
	GenericTest(t, "3A", 5, 5, true, true, false, -1, false)
}

func TestPersistPartition3A(t *testing.T) {
	// Test: restarts, partitions, many clients (3A) ...
	GenericTest(t, "3A", 5, 5, false, true, true, -1, false)
}

func TestPersistPartitionUnreliable3A(t *testing.T) {
	// Test: unreliable net, restarts, partitions, many clients (3A) ...
	GenericTest(t, "3A", 5, 5, true, true, true, -1, false)
}

func TestPersistPartitionUnreliableLinearizable3A(t *testing.T) {
	// Test: unreliable net, restarts, partitions, random keys, many clients (3A) ...
	GenericTest(t, "3A", 15, 7, true, true, true, -1, true)
}

//
// if one server falls behind, then rejoins, does it
// recover by using the InstallSnapshot RPC?
// also checks that majority discards committed log entries
// even if minority doesn't respond.
//
func TestSnapshotRPC3B(t *testing.T) {
	const nservers = 3
	maxraftstate := 1000
	cfg := make_config(t, nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: InstallSnapshot RPC (3B)")

	Put(cfg, ck, "a", "A", nil, -1)
	check(cfg, t, ck, "a", "A")

	// a bunch of puts into the majority partition.
	cfg.partition([]int{0, 1}, []int{2})
	{
		ck1 := cfg.makeClient([]int{0, 1})
		for i := 0; i < 50; i++ {
			Put(cfg, ck1, strconv.Itoa(i), strconv.Itoa(i), nil, -1)
		}
		time.Sleep(electionTimeout)
		Put(cfg, ck1, "b", "B", nil, -1)
	}

	// check that the majority partition has thrown away
	// most of its log entries.
	sz := cfg.LogSize()
	if sz > 8*maxraftstate {
		t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
	}

	// now make group that requires participation of
	// lagging server, so that it has to catch up.
	cfg.partition([]int{0, 2}, []int{1})
	{
		ck1 := cfg.makeClient([]int{0, 2})
		Put(cfg, ck1, "c", "C", nil, -1)
		Put(cfg, ck1, "d", "D", nil, -1)
		check(cfg, t, ck1, "a", "A")
		check(cfg, t, ck1, "b", "B")
		check(cfg, t, ck1, "1", "1")
		check(cfg, t, ck1, "49", "49")
	}

	// now everybody
	cfg.partition([]int{0, 1, 2}, []int{})

	Put(cfg, ck, "e", "E", nil, -1)
	check(cfg, t, ck, "c", "C")
	check(cfg, t, ck, "e", "E")
	check(cfg, t, ck, "1", "1")

	cfg.end()
}

// are the snapshots not too huge? 500 bytes is a generous bound for the
// operations we're doing here.
func TestSnapshotSize3B(t *testing.T) {
	const nservers = 3
	maxraftstate := 1000
	maxsnapshotstate := 500
	cfg := make_config(t, nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: snapshot size is reasonable (3B)")

	for i := 0; i < 200; i++ {
		Put(cfg, ck, "x", "0", nil, -1)
		check(cfg, t, ck, "x", "0")
		Put(cfg, ck, "x", "1", nil, -1)
		check(cfg, t, ck, "x", "1")
	}

	// check that servers have thrown away most of their log entries
	sz := cfg.LogSize()
	if sz > 8*maxraftstate {
		t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
	}

	// check that the snapshots are not unreasonably large
	ssz := cfg.SnapshotSize()
	if ssz > maxsnapshotstate {
		t.Fatalf("snapshot too large (%v > %v)", ssz, maxsnapshotstate)
	}

	cfg.end()
}

func TestSpeed3B(t *testing.T) {
	GenericTestSpeed(t, "3B", 1000)
}

func TestSnapshotRecover3B(t *testing.T) {
	// Test: restarts, snapshots, one client (3B) ...
	GenericTest(t, "3B", 1, 5, false, true, false, 1000, false)
}

func TestSnapshotRecoverManyClients3B(t *testing.T) {
	// Test: restarts, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 20, 5, false, true, false, 1000, false)
}

func TestSnapshotUnreliable3B(t *testing.T) {
	// Test: unreliable net, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, 5, true, false, false, 1000, false)
}

func TestSnapshotUnreliableRecover3B(t *testing.T) {
	// Test: unreliable net, restarts, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, 5, true, true, false, 1000, false)
}

func TestSnapshotUnreliableRecoverConcurrentPartition3B(t *testing.T) {
	// Test: unreliable net, restarts, partitions, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, 5, true, true, true, 1000, false)
}

func TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B(t *testing.T) {
	// Test: unreliable net, restarts, partitions, snapshots, random keys, many clients (3B) ...
	GenericTest(t, "3B", 15, 7, true, true, true, 1000, true)
}
