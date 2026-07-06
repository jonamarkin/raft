package raft

import (
	"testing"
	"time"
)

func TestElectionTimeout(t *testing.T) {
	server := NewServer()

	// Wait for a duration longer than the maximum election timeout to ensure the server has time to become a candidate
	time.Sleep(350 * time.Millisecond)

	server.mu.Lock()
	defer server.mu.Unlock()

	if server.state != Candidate {
		t.Errorf("Expected server state to be Candidate, got %v", server.state)
	}

	if server.currentTerm != 1 {
		t.Errorf("Expected current term to be 1, got %d", server.currentTerm)
	}
}

func TestHeartbeatResetsElectionTimer(t *testing.T) {
	server := NewServer()

	//Send a heartbeat every 100ms
	for i := 0; i < 5; i++ {
		time.Sleep(100 * time.Millisecond)
		server.ReceiveHeartbeat(server.currentTerm)
	}

	server.mu.Lock()
	defer server.mu.Unlock()

	if server.state != Follower {
		t.Errorf("Expected server state to be Follower, got %v", server.state)
	}

	if server.currentTerm != 0 {
		t.Errorf("Expected current term to be 0, got %d", server.currentTerm)
	}
}
