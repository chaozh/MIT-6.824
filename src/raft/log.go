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
	return len(l.LogEntries)
}

func (l *LogEntries) Append(e ...LogEntry) {
	l.LogEntries = append(l.LogEntries, e...)
}

func (l *LogEntries) Truncate(i int) {
	l.LogEntries = l.LogEntries[:i]
}

func (l *LogEntries) Slice(i int) []LogEntry {
	return l.LogEntries[i:]
}

func (l *LogEntries) Get(i int) LogEntry {
	return l.LogEntries[i]
}

func (l *LogEntries) Set(i int, e LogEntry) {
	l.LogEntries[i] = e
}

func (l *LogEntries) GetLastIndex() int {
	return l.Len() - 1
}

func (l *LogEntries) GetLastLog() LogEntry {
	return l.LogEntries[l.Len()-1]
}

func (l *LogEntries) String() string {
	nums := []string{}
	for _, e := range l.LogEntries {
		nums = append(nums, fmt.Sprintf("%4d", e.Term))
	}
	return fmt.Sprintf("[%s]", strings.Join(nums, " "))
}
