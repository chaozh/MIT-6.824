package mr

import "fmt"

type TaskPhase int

const (
	MapPhase TaskPhase = iota
	ReducePhase
)

type Task struct {
	FileName string
	NMap     int
	NReduce  int
	Phase    TaskPhase
	Sequence int
	Alive    bool
}

func getMapName(seq int, idx int) string {
	return fmt.Sprint("mr-", seq, "-", idx)
}

func getMergeName(idx int) string {
	return fmt.Sprint("mr-out-", idx)
}
