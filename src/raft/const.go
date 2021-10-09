package raft

import "time"

type RaftStatus int

const (
	RaftStatusFollower  RaftStatus = 1
	RaftStatusCandidate RaftStatus = 2
	RaftStatusLeader    RaftStatus = 3
)

const RaftElectionTimeout = 1000 * time.Millisecond
