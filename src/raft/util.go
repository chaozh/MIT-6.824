package raft

import (
	"log"

	"github.com/fatih/color"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func DPrintfCyan(format string, a ...interface{}) (n int, err error) {
	if Debug {
		color.Cyan(format, a...)
	}
	return
}
