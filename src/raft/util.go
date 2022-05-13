package raft

import (
	"log"
	"strings"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	str := strings.Join([]string{"[raft]:", format}, "")
	if Debug {
		log.Printf(str, a...)
	}
	return
}

func Max(a ...int) int {
	ans := a[0]
	for _, v := range a {
		if v > ans {
			ans = v
		}
	}
	return ans
}

func Min(a ...int) int {
	ans := a[0]
	for _, v := range a {
		if v < ans {
			ans = v
		}
	}
	return ans
}
