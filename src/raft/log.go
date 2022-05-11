package raft

import (
	"fmt"
	"strings"
)

type LogEntries struct {
	LogEntries []LogEntry
	Index0     int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// LogEntries[1] is the first entry in the log.
// LogEntries[0] saves the snapshot's meta,when there's no snapshot,it's nil.
// Once start to append log entries, the log entries will be saved in LogEntries[1:].
// Index0 is the same as lastIncludedIndex meaning in the raft paper.
func newLogEntries() LogEntries {
	return LogEntries{
		LogEntries: make([]LogEntry, 1),
		Index0:     0,
	}
}

func (e *LogEntry) String() string {
	return fmt.Sprintf("{Term: %d, Command: %v}", e.Term, e.Command)
}

func (l *LogEntries) Len() int {
	return l.Index0 + len(l.LogEntries)
}

func (l *LogEntries) Append(e ...LogEntry) {
	l.LogEntries = append(l.LogEntries, e...)
}

func (l *LogEntries) Truncate(i int) {
	l.LogEntries = l.LogEntries[:i-l.Index0]
}

//Get the log entries between [i:]
func (l *LogEntries) Slice(i int) []LogEntry {
	return l.LogEntries[i-l.Index0:]
}

func (l *LogEntries) Get(i int) LogEntry {
	if i < l.Index0 {
		DPrintf("ERROR: index smaller than log snapshot!i: %d, Index0: %d\n", i, l.Index0)
		panic("ERROR: index smaller than log snapshot!\n")
	} else if i > l.Index0+len(l.LogEntries) {
		panic("ERROR: index bigger than log entries!\n")
	}
	return l.LogEntries[i-l.Index0]
}

func (l *LogEntries) Set(i int, e LogEntry) {
	if i < l.Index0 {
		DPrintf("ERROR: index smaller than log snapshot!i: %d, Index0: %d\n", i, l.Index0)
		panic("ERROR: index smaller than log snapshot!\n")
	} else if i > l.Index0+len(l.LogEntries) {
		DPrintf("ERROR: index bigger than log entries!\n")
		panic("ERROR: index bigger than log entries!\n")
	}
	l.LogEntries[i-l.Index0] = e
}

//Discard the log entries before index.
func (l *LogEntries) Discard(i int) {
	DPrintf("discard(%d),Index0: %d,len of entries: %d\n", i, l.Index0, len(l.LogEntries))
	l.LogEntries = append([]LogEntry{}, l.LogEntries[i-l.Index0:]...)
	l.Index0 = i
	DPrintf("discard(%d) done,Index0: %d,len of entries: %d\n", i, l.Index0, len(l.LogEntries))
}

func (l *LogEntries) GetLastIndex() int {
	return l.Len() - 1
}

func (l *LogEntries) GetLastLog() LogEntry {
	return l.LogEntries[len(l.LogEntries)-1]
}

func (l *LogEntries) String() string {
	nums := []string{}
	for _, e := range l.LogEntries {
		nums = append(nums, fmt.Sprintf("%4d", e.Term))
	}
	return fmt.Sprintf("[%s]", strings.Join(nums, " "))
}
