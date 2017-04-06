package raftkv

import "testing"
import "strconv"
import "time"
import "fmt"
import "math/rand"
import "log"
import "strings"
import "sync/atomic"

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const electionTimeout = 1 * time.Second

func check(t *testing.T, ck *Clerk, key string, value string) {
	v := ck.Get(key)
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
// size) shouldn't exceed 2*maxraftstate.
func GenericTest(t *testing.T, tag string, nclients int, unreliable bool, crash bool, partitions bool, maxraftstate int) {
	const nservers = 5
	cfg := make_config(t, tag, nservers, unreliable, maxraftstate)
	defer cfg.cleanup()

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
			last := ""
			key := strconv.Itoa(cli)
			myck.Put(key, last)
			for atomic.LoadInt32(&done_clients) == 0 {
				if (rand.Int() % 1000) < 500 {
					nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
					// log.Printf("%d: client new append %v\n", cli, nv)
					myck.Append(key, nv)
					last = NextValue(last, nv)
					j++
				} else {
					// log.Printf("%d: client new get %v\n", cli, key)
					v := myck.Get(key)
					if v != last {
						log.Fatalf("get wrong value, key %v, wanted:\n%v\n, got\n%v\n", key, last, v)
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
			if j < 10 {
				log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			}
			key := strconv.Itoa(i)
			// log.Printf("Check %v for client %d\n", j, i)
			v := ck.Get(key)
			checkClntAppends(t, i, v, j)
		}

		if maxraftstate > 0 {
			// Check maximum after the servers have processed all client
			// requests and had time to checkpoint
			if cfg.LogSize() > 2*maxraftstate {
				t.Fatalf("logs were not trimmed (%v > 2*%v)", cfg.LogSize(), maxraftstate)
			}
		}
	}

	fmt.Printf("  ... Passed\n")
}

func TestBasic(t *testing.T) {
	fmt.Printf("Test: One client ...\n")
	GenericTest(t, "basic", 1, false, false, false, -1)
}

func TestConcurrent(t *testing.T) {
	fmt.Printf("Test: concurrent clients ...\n")
	GenericTest(t, "concur", 5, false, false, false, -1)
}

func TestUnreliable(t *testing.T) {
	fmt.Printf("Test: unreliable ...\n")
	GenericTest(t, "unreliable", 5, true, false, false, -1)
}

func TestUnreliableOneKey(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, "onekey", nservers, true, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	fmt.Printf("Test: Concurrent Append to same key, unreliable ...\n")

	ck.Put("k", "")

	const nclient = 5
	const upto = 10
	spawn_clients_and_wait(t, cfg, nclient, func(me int, myck *Clerk, t *testing.T) {
		n := 0
		for n < upto {
			myck.Append("k", "x "+strconv.Itoa(me)+" "+strconv.Itoa(n)+" y")
			n++
		}
	})

	var counts []int
	for i := 0; i < nclient; i++ {
		counts = append(counts, upto)
	}

	vx := ck.Get("k")
	checkConcurrentAppends(t, vx, counts)

	fmt.Printf("  ... Passed\n")
}

// Submit a request in the minority partition and check that the requests
// doesn't go through until the partition heals.  The leader in the original
// network ends up in the minority partition.
func TestOnePartition(t *testing.T) {
	const nservers = 5
	cfg := make_config(t, "partition", nservers, false, -1)
	defer cfg.cleanup()
	ck := cfg.makeClient(cfg.All())

	ck.Put("1", "13")

	fmt.Printf("Test: Progress in majority ...\n")

	p1, p2 := cfg.make_partition()
	cfg.partition(p1, p2)

	ckp1 := cfg.makeClient(p1)  // connect ckp1 to p1
	ckp2a := cfg.makeClient(p2) // connect ckp2a to p2
	ckp2b := cfg.makeClient(p2) // connect ckp2b to p2

	ckp1.Put("1", "14")
	check(t, ckp1, "1", "14")

	fmt.Printf("  ... Passed\n")

	done0 := make(chan bool)
	done1 := make(chan bool)

	fmt.Printf("Test: No progress in minority ...\n")
	go func() {
		ckp2a.Put("1", "15")
		done0 <- true
	}()
	go func() {
		ckp2b.Get("1") // different clerk in p2
		done1 <- true
	}()

	select {
	case <-done0:
		t.Fatalf("Put in minority completed")
	case <-done1:
		t.Fatalf("Get in minority completed")
	case <-time.After(time.Second):
	}

	check(t, ckp1, "1", "14")
	ckp1.Put("1", "16")
	check(t, ckp1, "1", "16")

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Completion after heal ...\n")

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

	check(t, ck, "1", "15")

	fmt.Printf("  ... Passed\n")
}

func TestManyPartitionsOneClient(t *testing.T) {
	fmt.Printf("Test: many partitions ...\n")
	GenericTest(t, "manypartitions", 1, false, false, true, -1)
}

func TestManyPartitionsManyClients(t *testing.T) {
	fmt.Printf("Test: many partitions, many clients ...\n")
	GenericTest(t, "manypartitionsclnts", 5, false, false, true, -1)
}

func TestPersistOneClient(t *testing.T) {
	fmt.Printf("Test: persistence with one client ...\n")
	GenericTest(t, "persistone", 1, false, true, false, -1)
}

func TestPersistConcurrent(t *testing.T) {
	fmt.Printf("Test: persistence with concurrent clients ...\n")
	GenericTest(t, "persistconcur", 5, false, true, false, -1)
}

func TestPersistConcurrentUnreliable(t *testing.T) {
	fmt.Printf("Test: persistence with concurrent clients, unreliable ...\n")
	GenericTest(t, "persistconcurunreliable", 5, true, true, false, -1)
}

func TestPersistPartition(t *testing.T) {
	fmt.Printf("Test: persistence with concurrent clients and repartitioning servers...\n")
	GenericTest(t, "persistpart", 5, false, true, true, -1)
}

func TestPersistPartitionUnreliable(t *testing.T) {
	fmt.Printf("Test: persistence with concurrent clients and repartitioning servers, unreliable...\n")
	GenericTest(t, "persistpartunreliable", 5, true, true, true, -1)
}

//
// if one server falls behind, then rejoins, does it
// recover by using the InstallSnapshot RPC?
// also checks that majority discards committed log entries
// even if minority doesn't respond.
//
func TestSnapshotRPC(t *testing.T) {
	const nservers = 3
	maxraftstate := 1000
	cfg := make_config(t, "snapshotrpc", nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	fmt.Printf("Test: InstallSnapshot RPC ...\n")

	ck.Put("a", "A")
	check(t, ck, "a", "A")

	// a bunch of puts into the majority partition.
	cfg.partition([]int{0, 1}, []int{2})
	{
		ck1 := cfg.makeClient([]int{0, 1})
		for i := 0; i < 50; i++ {
			ck1.Put(strconv.Itoa(i), strconv.Itoa(i))
		}
		time.Sleep(electionTimeout)
		ck1.Put("b", "B")
	}

	// check that the majority partition has thrown away
	// most of its log entries.
	if cfg.LogSize() > 2*maxraftstate {
		t.Fatalf("logs were not trimmed (%v > 2*%v)", cfg.LogSize(), maxraftstate)
	}

	// now make group that requires participation of
	// lagging server, so that it has to catch up.
	cfg.partition([]int{0, 2}, []int{1})
	{
		ck1 := cfg.makeClient([]int{0, 2})
		ck1.Put("c", "C")
		ck1.Put("d", "D")
		check(t, ck1, "a", "A")
		check(t, ck1, "b", "B")
		check(t, ck1, "1", "1")
		check(t, ck1, "49", "49")
	}

	// now everybody
	cfg.partition([]int{0, 1, 2}, []int{})

	ck.Put("e", "E")
	check(t, ck, "c", "C")
	check(t, ck, "e", "E")
	check(t, ck, "1", "1")

	fmt.Printf("  ... Passed\n")
}

// are the snapshots not too huge? 500 bytes is a generous bound for the
// operations we're doing here.
func TestSnapshotSize(t *testing.T) {
	const nservers = 3
	maxraftstate := 1000
	maxsnapshotstate := 500
	cfg := make_config(t, "snapshotsize", nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	fmt.Printf("Test: snapshot size is reasonable ...\n")

	for i := 0; i < 200; i++ {
		ck.Put("x", "0")
		check(t, ck, "x", "0")
		ck.Put("x", "1")
		check(t, ck, "x", "1")
	}

	// check that servers have thrown away most of their log entries
	if cfg.LogSize() > 2*maxraftstate {
		t.Fatalf("logs were not trimmed (%v > 2*%v)", cfg.LogSize(), maxraftstate)
	}

	// check that the snapshots are not unreasonably large
	if cfg.SnapshotSize() > maxsnapshotstate {
		t.Fatalf("snapshot too large (%v > %v)", cfg.SnapshotSize(), maxsnapshotstate)
	}

	fmt.Printf("  ... Passed\n")
}

func TestSnapshotRecover(t *testing.T) {
	fmt.Printf("Test: persistence with one client and snapshots ...\n")
	GenericTest(t, "snapshot", 1, false, true, false, 1000)
}

func TestSnapshotRecoverManyClients(t *testing.T) {
	fmt.Printf("Test: persistence with several clients and snapshots ...\n")
	GenericTest(t, "snapshotunreliable", 20, false, true, false, 1000)
}

func TestSnapshotUnreliable(t *testing.T) {
	fmt.Printf("Test: persistence with several clients, snapshots, unreliable ...\n")
	GenericTest(t, "snapshotunreliable", 5, true, false, false, 1000)
}

func TestSnapshotUnreliableRecover(t *testing.T) {
	fmt.Printf("Test: persistence with several clients, failures, and snapshots, unreliable ...\n")
	GenericTest(t, "snapshotunreliablecrash", 5, true, true, false, 1000)
}

func TestSnapshotUnreliableRecoverConcurrentPartition(t *testing.T) {
	fmt.Printf("Test: persistence with several clients, failures, and snapshots, unreliable and partitions ...\n")
	GenericTest(t, "snapshotunreliableconcurpartitions", 5, true, true, true, 1000)
}
