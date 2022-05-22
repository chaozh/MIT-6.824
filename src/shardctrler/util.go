package shardctrler

import (
	"log"
	"strings"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	str := strings.Join([]string{"[shardctrler]:", format}, "")
	if Debug {
		log.Printf(str, a...)
	}
	return
}
