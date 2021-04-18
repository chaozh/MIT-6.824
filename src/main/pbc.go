package main

//
// pbservice client application
//
// export GOPATH=~/6.824
// go build viewd.go
// go build pbd.go
// go build pbc.go
// ./viewd /tmp/rtm-v &
// ./pbd /tmp/rtm-v /tmp/rtm-1 &
// ./pbd /tmp/rtm-v /tmp/rtm-2 &
// ./pbc /tmp/rtm-v key1 value1
// ./pbc /tmp/rtm-v key1
//
// change "rtm" to your user name.
// start the pbd programs in separate windows and kill
// and restart them to exercise fault tolerance.
//

import "6.824/pbservice"
import "os"
import "fmt"

func usage() {
	fmt.Printf("Usage: pbc viewport key\n")
	fmt.Printf("       pbc viewport key value\n")
	os.Exit(1)
}

func main() {
	if len(os.Args) == 3 {
		// get
		ck := pbservice.MakeClerk(os.Args[1], "")
		v := ck.Get(os.Args[2])
		fmt.Printf("%v\n", v)
	} else if len(os.Args) == 4 {
		// put
		ck := pbservice.MakeClerk(os.Args[1], "")
		ck.Put(os.Args[2], os.Args[3])
	} else {
		usage()
	}
}
