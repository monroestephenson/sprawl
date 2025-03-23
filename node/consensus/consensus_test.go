package consensus

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// testCluster represents a cluster of nodes for testing
type testCluster struct {
	nodes               []*RaftNode
	mu                  sync.RWMutex
	replicationManagers []*ReplicationManager
}

func newTestCluster(size int) *testCluster {
	tc := &testCluster{
		nodes:               make([]*RaftNode, size),
		replicationManagers: make([]*ReplicationManager, size),
	}

	// Create nodes
	for i := 0; i < size; i++ {
		nodeID := fmt.Sprintf("node%d", i+1)
		tc.nodes[i] = NewRaftNode(nodeID, nil)
	}

	// Set up peer connections
	for i, node := range tc.nodes {
		peers := make([]string, 0)
		for j := 0; j < size; j++ {
			if j != i {
				peers = append(peers, tc.nodes[j].id)
			}
		}
		node.UpdatePeers(peers)

		// Create replication manager
		tc.replicationManagers[i] = NewReplicationManager(node.id, node, 2)

		// Override vote and append channels with direct method calls
		node.sendVoteRequest = func(peer string, req VoteRequest) VoteResponse {
			tc.mu.RLock()
			defer tc.mu.RUnlock()
			for _, n := range tc.nodes {
				if n.id == peer {
					n.mu.Lock()
					defer n.mu.Unlock()
					resp := VoteResponse{Term: n.term, VoteGranted: false}
					if req.Term < n.term {
						return resp
					}
					if req.Term > n.term {
						n.term = req.Term
						n.state = Follower
						n.votedFor = ""
						n.leaderId = ""
					}
					if (n.votedFor == "" || n.votedFor == req.CandidateId) && n.state != Leader {
						n.votedFor = req.CandidateId
						resp.VoteGranted = true
						n.resetElectionTimer()
					}
					return resp
				}
			}
			return VoteResponse{Term: 0, VoteGranted: false}
		}

		node.sendAppendEntries = func(peer string, req AppendEntriesRequest) AppendEntriesResponse {
			tc.mu.RLock()
			defer tc.mu.RUnlock()
			for _, n := range tc.nodes {
				if n.id == peer {
					n.mu.Lock()
					defer n.mu.Unlock()
					resp := AppendEntriesResponse{Term: n.term, Success: false}
					if req.Term < n.term {
						return resp
					}
					if req.Term >= n.term {
						n.leaderId = req.LeaderId
						n.term = req.Term
						if n.state != Follower {
							n.state = Follower
							n.votedFor = ""
						}
						n.resetElectionTimer()
						resp.Success = true
					}
					return resp
				}
			}
			return AppendEntriesResponse{Term: 0, Success: false}
		}
	}

	return tc
}

func (tc *testCluster) start() {
	for i := range tc.nodes {
		go tc.nodes[i].run()
	}
}

func (tc *testCluster) stop() {
	for i := range tc.nodes {
		tc.nodes[i].Stop()
		tc.replicationManagers[i].Stop()
	}
}

func (tc *testCluster) getLeader() (*RaftNode, *ReplicationManager) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	for i, node := range tc.nodes {
		if node.IsLeader() {
			return node, tc.replicationManagers[i]
		}
	}
	return nil, nil
}

func TestRaftLeaderElection(t *testing.T) {
	// Set test mode environment variable
	os.Setenv("SPRAWL_TEST_MODE", "1")
	defer os.Unsetenv("SPRAWL_TEST_MODE")

	cluster := newTestCluster(3)
	cluster.start()
	defer cluster.stop()

	// Wait for leader election with timeout
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	var leader *RaftNode
	var rm *ReplicationManager
	for {
		select {
		case <-deadline:
			t.Fatal("Timeout waiting for leader election")
		case <-ticker.C:
			leader, rm = cluster.getLeader()
			if leader != nil && rm != nil {
				goto LeaderElected
			}
		}
	}

LeaderElected:
	// Verify that exactly one leader was elected and has a replication manager
	leaderCount := 0
	var leaderID string
	for _, node := range cluster.nodes {
		if node.IsLeader() {
			leaderCount++
			leaderID = node.id
		}
	}

	if leaderCount != 1 {
		t.Errorf("Expected exactly one leader, got %d", leaderCount)
	}

	// Verify that all nodes recognize the same leader
	for _, node := range cluster.nodes {
		if node.GetLeader() != leaderID {
			t.Errorf("Node %s recognizes leader %s, expected %s", node.id, node.GetLeader(), leaderID)
		}
	}

	// Verify that the leader's replication manager is initialized
	if rm == nil {
		t.Error("Leader's replication manager is nil")
	}
}

func TestReplication(t *testing.T) {
	// Set test mode environment variable
	os.Setenv("SPRAWL_TEST_MODE", "1")
	defer os.Unsetenv("SPRAWL_TEST_MODE")

	cluster := newTestCluster(3)
	cluster.start()
	defer cluster.stop()

	// Wait for leader election with timeout
	var leader *RaftNode
	var leaderRM *ReplicationManager
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatal("Timeout waiting for leader election")
		case <-ticker.C:
			leader, leaderRM = cluster.getLeader()
			if leader != nil {
				goto LeaderElected
			}
		}
	}

LeaderElected:
	if leader == nil || leaderRM == nil {
		t.Fatal("No leader elected")
	}

	// Test message replication
	testTopic := "test-topic"
	testPayload := []byte("test message")

	// Set up a channel to track committed entries
	committed := make(chan ReplicationEntry, 3)
	for _, rm := range cluster.replicationManagers {
		rm.SetCommitCallback(func(entry ReplicationEntry) {
			select {
			case committed <- entry:
			default:
			}
		})
	}

	// Propose an entry through the leader
	err := leaderRM.ProposeEntry(testTopic, testPayload)
	if err != nil {
		t.Fatalf("Failed to propose entry: %v", err)
	}

	// Wait for replication with timeout
	deadline = time.After(5 * time.Second)
	replicatedCount := 0
	expectedReplicas := 2 // Replication factor

	for replicatedCount < expectedReplicas {
		select {
		case <-deadline:
			t.Fatalf("Timeout waiting for replication (got %d/%d replicas)", replicatedCount, expectedReplicas)
		case entry := <-committed:
			if string(entry.Payload) != string(testPayload) {
				t.Errorf("Got payload %s, expected %s", string(entry.Payload), string(testPayload))
			}
			if entry.Topic != testTopic {
				t.Errorf("Got topic %s, expected %s", entry.Topic, testTopic)
			}
			replicatedCount++
		}
	}
}

func TestReplicationWithNodeFailure(t *testing.T) {
	// Set test mode environment variable
	os.Setenv("SPRAWL_TEST_MODE", "1")
	defer os.Unsetenv("SPRAWL_TEST_MODE")

	cluster := newTestCluster(3)
	cluster.start()
	defer cluster.stop()

	// Wait for leader election with timeout
	var leader *RaftNode
	var leaderRM *ReplicationManager
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatal("Timeout waiting for leader election")
		case <-ticker.C:
			leader, leaderRM = cluster.getLeader()
			if leader != nil && leaderRM != nil {
				goto LeaderElected
			}
		}
	}

LeaderElected:
	if leader == nil || leaderRM == nil {
		t.Fatal("No leader elected")
	}

	var leaderIndex int
	for i, node := range cluster.nodes {
		if node.id == leader.id {
			leaderIndex = i
			break
		}
	}

	// Stop one follower
	followerIndex := (leaderIndex + 1) % 3
	cluster.nodes[followerIndex].Stop()
	cluster.replicationManagers[followerIndex].Stop()

	// Test message replication
	testTopic := "test-topic"
	testPayload := []byte("test message")

	// Set up a channel to track committed entries
	committed := make(chan ReplicationEntry, 2) // Increased buffer size
	remainingIndex := (leaderIndex + 2) % 3

	// Set commit callback for both leader and remaining follower
	leaderRM.SetCommitCallback(func(entry ReplicationEntry) {
		select {
		case committed <- entry:
		default:
		}
	})

	cluster.replicationManagers[remainingIndex].SetCommitCallback(func(entry ReplicationEntry) {
		select {
		case committed <- entry:
		default:
		}
	})

	// Propose an entry through the leader
	err := leaderRM.ProposeEntry(testTopic, testPayload)
	if err != nil {
		t.Fatalf("Failed to propose entry: %v", err)
	}

	// Wait for replication with timeout
	replicatedCount := 0
	expectedReplicas := 2 // Leader and one follower
	deadline = time.After(5 * time.Second)

	for replicatedCount < expectedReplicas {
		select {
		case entry := <-committed:
			if string(entry.Payload) != string(testPayload) {
				t.Errorf("Got payload %s, expected %s", string(entry.Payload), string(testPayload))
			}
			if entry.Topic != testTopic {
				t.Errorf("Got topic %s, expected %s", entry.Topic, testTopic)
			}
			replicatedCount++
		case <-deadline:
			t.Fatalf("Timeout waiting for replication (got %d/%d replicas)", replicatedCount, expectedReplicas)
		}
	}
}
