package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	_ "github.com/mattn/go-sqlite3"

	"github.com/Prem05J/raft-leader/internal/fsm"
	httpserver "github.com/Prem05J/raft-leader/internal/http"
	"github.com/Prem05J/raft-leader/internal/raftstore"
)

const (
	MinElectionTimeout  = 250 * time.Millisecond
	MaxElectionTimeout  = 500 * time.Millisecond
	HeartbeatInterval   = 100 * time.Millisecond
	LeaderLeaseTimeout  = 50 * time.Millisecond
	CommitTimeout       = 50 * time.Millisecond
	SnapshotInterval    = 30 * time.Second
	ConnectionPoolCount = 5
	ConnectionTimeout   = 10 * time.Second
)

func main() {
	var (
		id       = flag.String("id", "", "nodeId")
		raftAddr = flag.String("raft-addr", "127.0.0.1:12000", "raft bind address")
		httpAddr = flag.String("http-addr", "127.0.0.1:9000", "http bind address")
		dataDir  = flag.String("data-dir", "./data", "data dir")
		joinAddr = flag.String("join", "", "leader to join (optional)")
	)
	flag.Parse()

	if *id == "" {
		log.Fatal("id required")
	}

	os.MkdirAll(*dataDir, 0755)
	dbPath := filepath.Join(*dataDir, fmt.Sprintf("node-%s.sqlite", *id))

	store, err := raftstore.NewSQLiteStore(dbPath, 50, time.Second)
	if err != nil {
		log.Fatalf("new sqlite store: %v", err)
	}

	applyFSM, err := fsm.NewFSM(dbPath)
	if err != nil {
		log.Fatalf("new fsm: %v", err)
	}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(*id)
	config.ElectionTimeout = time.Duration(rand.Int63n(int64(MaxElectionTimeout-MinElectionTimeout))) + MinElectionTimeout
	config.HeartbeatTimeout = HeartbeatInterval
	config.LeaderLeaseTimeout = LeaderLeaseTimeout
	config.CommitTimeout = CommitTimeout
	config.SnapshotInterval = SnapshotInterval
	config.LogLevel = "INFO"

	log.Printf("Node %s configured with election timeout: %v", *id, config.ElectionTimeout)

	address, _ := net.ResolveTCPAddr("tcp", *raftAddr)
	transport, err := raft.NewTCPTransport(*raftAddr, address, ConnectionPoolCount, ConnectionTimeout, os.Stdout)
	if err != nil {
		log.Fatalf("transport: %v", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(*dataDir, 2, os.Stdout)
	if err != nil {
		log.Fatalf("snapshots: %v", err)
	}

	raftNode, err := raft.NewRaft(config, applyFSM, store, store, snapshots, transport)
	if err != nil {
		log.Fatalf("new raft: %v", err)
	}

	if *joinAddr == "" {
		bootstrapCluster(raftNode, config.LocalID, raftAddr)
	} else {
		joinCluster(*joinAddr, *id, *raftAddr, raftNode)
	}

	go enhancedClusterMonitor(raftNode, *id, *raftAddr)

	srv := httpserver.NewServer(*id, raftNode, applyFSM, *httpAddr, *raftAddr)
	go func() {
		log.Printf("HTTP server listening %s", *httpAddr)
		if err := srv.ListenAndServe(); err != nil {
			log.Fatalf("http serve: %v", err)
		}
	}()

	select {}
}

func bootstrapCluster(raftNode *raft.Raft, localID raft.ServerID, raftAddr *string) {
	cfg := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:       localID,
				Address:  raft.ServerAddress(*raftAddr),
				Suffrage: raft.Voter,
			},
		},
	}

	log.Printf("Bootstrapping node: %s at %s", localID, *raftAddr)

	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		future := raftNode.BootstrapCluster(cfg)
		if err := future.Error(); err != nil {
			if err == raft.ErrCantBootstrap {
				log.Printf("Cluster already bootstrapped")
				break
			}
			retryDelay := time.Duration(i+1) * time.Second
			log.Printf("Bootstrap attempt %d failed: %v, retrying in %v", i+1, err, retryDelay)
			time.Sleep(retryDelay)
		} else {
			log.Printf("Bootstrap successful")
			break
		}
	}
}

func joinCluster(joinAddr, id, raftAddr string, raftNode *raft.Raft) {
	log.Printf("Joining existing cluster via: %s", joinAddr)

	maxRetries := 15
	retryDelay := 1 * time.Second

	for i := 0; i < maxRetries; i++ {
		joinURL := fmt.Sprintf("http://%s/join?id=%s&addr=%s", joinAddr, id, raftAddr)
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Post(joinURL, "text/plain", nil)

		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			if err := waitForClusterMembership(raftNode, raft.ServerID(id), 15*time.Second); err != nil {
				log.Printf("Membership confirmation: %v", err)
			}
			return
		}

		if err != nil {
			log.Printf("Join attempt %d failed: %v", i+1, err)
		} else {
			log.Printf("Join attempt %d failed with status: %s", i+1, resp.Status)
			resp.Body.Close()
		}

		if i == maxRetries-1 {
			log.Printf("Failed to join cluster after %d attempts, continuing as standalone", maxRetries)
			return
		}

		time.Sleep(retryDelay)
		retryDelay = time.Duration(float64(retryDelay) * 1.5)
	}
}

func waitForClusterMembership(raftNode *raft.Raft, expectedID raft.ServerID, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for now := range ticker.C {
		if now.After(deadline) {
			return fmt.Errorf("timeout waiting for cluster membership")
		}

		future := raftNode.GetConfiguration()
		if err := future.Error(); err != nil {
			continue
		}

		config := future.Configuration()
		for _, server := range config.Servers {
			if server.ID == expectedID {
				log.Printf("Node %s confirmed in cluster configuration", expectedID)
				return nil
			}
		}
	}
	return nil
}

func enhancedClusterMonitor(raftNode *raft.Raft, nodeID, raftAddr string) {
	time.Sleep(2 * time.Second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	var lastLeader raft.ServerAddress
	var lastState raft.RaftState
	var electionCount int
	var lastElectionTime time.Time

	log.Printf("Starting cluster monitor for node %s", nodeID)

	for range ticker.C {
		currentState := raftNode.State()
		currentLeader := raftNode.Leader()
		currentTerm := raftNode.Stats()["term"]

		if currentState != lastState {
			log.Printf("STATE CHANGE: %s -> %s (term: %s)", lastState, currentState, currentTerm)
			lastState = currentState

			if currentState == raft.Candidate {
				electionCount++
				lastElectionTime = time.Now()
				log.Printf("ELECTION STARTED: Attempt #%d", electionCount)
			}
		}

		if currentLeader != lastLeader {
			if currentLeader == "" {
				log.Printf("LEADER LOST: No current leader")
			} else {
				log.Printf("NEW LEADER: %s (term: %s)", currentLeader, currentTerm)
			}
			lastLeader = currentLeader
		}

		if currentState == raft.Candidate && time.Since(lastElectionTime) > 2*time.Second {
			log.Printf("LONG ELECTION: Candidate for %v, may indicate network issues", time.Since(lastElectionTime))
		}

		if time.Now().Second()%10 == 0 {
			stats := raftNode.Stats()
			log.Printf("Stats - State: %s, Leader: %s, Term: %s, Commit: %s",
				currentState, currentLeader, stats["term"], stats["commit_index"])
		}

		if currentState == raft.Leader && time.Since(startTime) > 10*time.Second {
			checkClusterHealth(raftNode, nodeID)
		}
	}
}

var startTime = time.Now()

func checkClusterHealth(raftNode *raft.Raft, nodeID string) {
	future := raftNode.GetConfiguration()
	if err := future.Error(); err != nil {
		log.Printf("Health check failed: %v", err)
		return
	}

	config := future.Configuration()
	stats := raftNode.Stats()

	log.Printf("Cluster Health - Nodes: %d, Term: %s, Commit: %s",
		len(config.Servers), stats["term"], stats["commit_index"])

	if len(config.Servers) > 1 {
		quorum := (len(config.Servers) / 2) + 1
		log.Printf("Quorum required: %d nodes", quorum)
	}
}
