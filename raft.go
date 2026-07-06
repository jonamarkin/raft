package raft

import (
	"math/rand"
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

// NewServer initializes a new node and starts its election timer
func NewServer() *Server {
	s := &Server{
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		lastContact: time.Now(),
	}

	go s.runElectionTimer()

	return s
}

// runElectionTimer runs the election timer for the server
func (s *Server) runElectionTimer() {
	//Set a random election timeout between 150ms and 300ms
	timeout := time.Duration(150+rand.Intn(150)) * time.Millisecond

	//Check the clock every 10ms to see if the election timeout has been reached
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C

		s.mu.Lock()

		//If the server became a leader we do not need to run the election timer anymore
		if s.state == Leader {
			s.mu.Unlock()
			return
		}

		//If the election timeout has been reached, start a new election
		if time.Since(s.lastContact) >= timeout {
			s.startElection()
			s.mu.Unlock()
			return
		}

		s.mu.Unlock()
	}
}

// startElection to transition the server to a candidate state
func (s *Server) startElection() {
	s.state = Candidate
	s.currentTerm++
	s.votedFor = 1 //Assuming this server's ID is 1 for simplicity
	//Later on, we would send RequestVote RPCs to other servers and wait for votes
}

// ReceiveHeartbeat to handle incoming heartbeats from the leader
func (s *Server) ReceiveHeartbeat(term int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	//Reset the timer and ensure we are in the follower state
	s.lastContact = time.Now()
	s.state = Follower

}
