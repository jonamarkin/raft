package raft

import (
	"testing"
	"time"
)

func TestElectionTimeout(t *testing.T) {
	server := NewServer(1, []int{2, 3, 4})

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
	server := NewServer(1, []int{2, 3, 4})

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

func TestRequestVote(t *testing.T) {
	server := NewServer(1, []int{2, 3, 4})
	server.currentTerm = 1

	// Stop the election timer to prevent it from interfering with the test
	server.mu.Lock()
	server.lastContact = time.Now().Add(-time.Hour) // Simulate that the server hasn't heard from a leader for a long time
	server.mu.Unlock()

	//First scenario: Reject an outdated vote request
	args1 := RequestVoteArgs{
		Term:        0,
		CandidateId: 2,
	}
	reply1 := RequestVoteReply{}
	server.RequestVote(args1, &reply1)

	if reply1.VoteGranted {
		t.Errorf("Expected vote to be rejected for outdated term, but it was granted")
	}

	//Second scenario: Grant a vote for a new term
	args2 := RequestVoteArgs{
		Term:        2,
		CandidateId: 3,
	}
	reply2 := RequestVoteReply{}
	server.RequestVote(args2, &reply2)

	if !reply2.VoteGranted {
		t.Errorf("Expected vote to be granted for new term, but it was rejected")
	}

	if server.votedFor != 3 {
		t.Errorf("Expected votedFor to be 3, got %d", server.votedFor)
	}

	//Third scenario: Reject a vote request from the same term after already voting
	args3 := RequestVoteArgs{
		Term:        2,
		CandidateId: 4,
	}
	reply3 := RequestVoteReply{}
	server.RequestVote(args3, &reply3)

	if reply3.VoteGranted {
		t.Errorf("Expected vote to be rejected for same term after already voting, but it was granted")
	}

	if server.votedFor != 3 {
		t.Errorf("Expected votedFor to still be 3, got %d", server.votedFor)
	}

}

func TestFollowerReceivesHeartbeat(t *testing.T) {
	server := NewServer(1, []int{2, 3, 4})
	server.currentTerm = 2

	//Scenario where a valid heartbeat is received from the leader
	args := AppendEntriesArgs{
		Term:     2,
		LeaderId: 2,
	}
	reply := AppendEntriesReply{}
	server.AppendEntries(args, &reply)

	if !reply.Success {
		t.Errorf("Expected heartbeat to be successful, but it failed")
	}
	if server.state != Follower {
		t.Errorf("Expected server state to be Follower after receiving heartbeat, got %v", server.state)
	}

	//Scenario where a heartbeat is received from an older leader
	argsOld := AppendEntriesArgs{
		Term:     1,
		LeaderId: 3,
	}
	replyOld := AppendEntriesReply{}
	server.AppendEntries(argsOld, &replyOld)

	if replyOld.Success {
		t.Errorf("Expected heartbeat from older leader to fail, but it succeeded")
	}

}
