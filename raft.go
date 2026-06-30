package raft

import (
	"sync"
	"time"
)

// State to represent the state of a Raft node
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// Server to represent a single Raft node
type Server struct {
	mu sync.Mutex

	state       State
	currentTerm int
	votedFor    int

	//lastContact to keep track of the last time the server received a message from the leader
	lastContact time.Time
}
