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

	// Identity and routing
	serverId int
	peerIds  []int

	state       State
	currentTerm int
	votedFor    int

	//lastContact to keep track of the last time the server received a message from the leader
	lastContact time.Time
}

// NewServer initializes a new node and starts its election timer
func NewServer(id int, peerIds []int) *Server {
	s := &Server{
		serverId:    id,
		peerIds:     peerIds,
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
	s.votedFor = s.serverId

	votesReceived := 1 // Vote for self

	//Args for the RequestVote RPC
	args := RequestVoteArgs{
		Term:        s.currentTerm,
		CandidateId: s.serverId,
	}

	//Save the current term we are starting with so we can abort the election if we receive a higher term
	savedCurrentTerm := s.currentTerm

	//Loop through all peers and send them a RequestVote RPC
	for _, peerId := range s.peerIds {
		//Send the RequestVote RPC in a separate goroutine to avoid blocking for each peer
		go func(peer int) {
			reply := RequestVoteReply{}

			// Simulate sending the RequestVote RPC to the peer
			ok := s.sendRequestVote(peer, args, &reply)

			if ok {
				s.mu.Lock()
				defer s.mu.Unlock()

				// If our state changed while waiting for the reply, we should ignore it
				if s.state != Candidate || s.currentTerm != savedCurrentTerm {
					return
				}

				//If the reply's term is greater than our current term, we need to step down to a follower
				if reply.Term > s.currentTerm {
					s.currentTerm = reply.Term
					s.state = Follower
					s.votedFor = -1
					return
				}

				//If the vote was granted, increment the votes received
				if reply.VoteGranted {
					votesReceived++
					//If we have received a majority of votes, become the leader
					totalServers := len(s.peerIds) + 1
					if votesReceived > totalServers/2 {
						s.startLeader()
					}
				}
			}
		}(peerId)

	}

}

// ReceiveHeartbeat to handle incoming heartbeats from the leader
func (s *Server) ReceiveHeartbeat(term int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	//Reset the timer and ensure we are in the follower state
	s.lastContact = time.Now()
	s.state = Follower

}

//RPC Stuff

// RequestVoteArgs represents the arguments for a RequestVote RPC
type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

// RequestVoteReply represents the reply for a RequestVote RPC
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote handles incoming RequestVote RPCs
func (s *Server) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	//If the term in the request is less than the current term, reject the vote
	if args.Term < s.currentTerm {
		reply.Term = s.currentTerm
		reply.VoteGranted = false
		return nil
	}

	//If the term in the request is greater than the current term, update the current term and reset the vote
	if args.Term > s.currentTerm {
		s.currentTerm = args.Term
		s.state = Follower
		s.votedFor = -1
	}

	//If the server hasn't voted for anyone in this term, grant the vote
	if s.votedFor == -1 || s.votedFor == args.CandidateId {
		s.votedFor = args.CandidateId
		reply.VoteGranted = true
		s.lastContact = time.Now()
	} else {
		reply.VoteGranted = false
	}

	reply.Term = s.currentTerm
	return nil

}

func (s *Server) sendRequestVote(peerId int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	// Simulate sending the RequestVote RPC to the peer
	// In a real implementation, this would involve network communication
	// For testing purposes, we can assume the RPC is always successful
	return true
}

func (s *Server) startLeader() {
	s.state = Leader
	// Additional logic for starting leader duties would go here (e.g., sending heartbeats)
}
