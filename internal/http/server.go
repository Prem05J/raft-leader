package httpserver

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	batcher "github.com/Prem05J/raft-leader/batcher"
	"github.com/Prem05J/raft-leader/internal/fsm"
	"github.com/hashicorp/raft"
)

type Server struct {
	nodeID   string
	raft     *raft.Raft
	fsm      *fsm.FSM
	httpAddr string
	raftAddr string
	batcher  batcher.Batcher
}

func NewServer(nodeID string, raftNode *raft.Raft, fsm *fsm.FSM, httpAddr, raftAddr string) *http.Server {
	s := &Server{
		nodeID:   nodeID,
		raft:     raftNode,
		fsm:      fsm,
		httpAddr: httpAddr,
		raftAddr: raftAddr,
		batcher:  *batcher.NewBatcher(raftNode, 600, 1*time.Millisecond),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/join", s.joinHandler)
	mux.HandleFunc("/flush", func(w http.ResponseWriter, r *http.Request) {
		s.batcher.SendBatch()
		w.Write([]byte("batch flushed"))
	})

	mux.HandleFunc("/set", s.setHandler)
	mux.HandleFunc("/get", s.getHandler)
	mux.HandleFunc("/cluster", s.clusterHandler)
	mux.HandleFunc("/health", s.healthHandler)

	return &http.Server{
		Addr:    httpAddr,
		Handler: mux,
	}
}

func (s *Server) joinHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	id := r.URL.Query().Get("id")
	addr := r.URL.Query().Get("addr")

	if id == "" || addr == "" {
		http.Error(w, "id and addr required", http.StatusBadRequest)
		return
	}

	if s.raft.State() != raft.Leader {
		leaderAddr := s.raft.Leader()
		if leaderAddr == "" {
			http.Error(w, "no leader available", http.StatusServiceUnavailable)
			return
		}

		leaderHTTPAddr := strings.Replace(string(leaderAddr), "1200", "900", 1)
		redirectURL := fmt.Sprintf("http://%s/join?id=%s&addr=%s", leaderHTTPAddr, id, addr)

		http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
		return
	}

	future := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(addr), 0, 0)
	if err := future.Error(); err != nil {
		http.Error(w, fmt.Sprintf("failed to add voter: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "node %s added successfully", id)
	log.Printf("Added node %s at %s to cluster", id, addr)
}

func (s *Server) setHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var data map[string]string
	if err := json.Unmarshal(body, &data); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	key, ok := data["key"]
	value, ok2 := data["value"]
	if !ok || !ok2 {
		http.Error(w, "key and value required", http.StatusBadRequest)
		return
	}

	if s.raft.State() != raft.Leader {
		leaderAddr := s.raft.Leader()
		if leaderAddr == "" {
			http.Error(w, "no leader available", http.StatusServiceUnavailable)
			return
		}

		leaderHTTPAddr := strings.Replace(string(leaderAddr), "1200", "900", 1)
		redirectURL := fmt.Sprintf("http://%s/set", leaderHTTPAddr)

		resp, err := http.Post(redirectURL, "application/json", strings.NewReader(string(body)))
		if err != nil {
			http.Error(w, fmt.Sprintf("forward failed: %v", err), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		return
	}

	command := map[string]string{
		"op":    "set",
		"key":   key,
		"value": value,
	}
	commandData, _ := json.Marshal(command)

	erro := s.batcher.Apply(commandData)
	if erro != nil {
		http.Error(w, erro.Error(), http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "key %s set successfully", key)
}

func (s *Server) getHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key required", http.StatusBadRequest)
		return
	}

	value, err := s.fsm.Get(key)
	if err != nil {
		http.Error(w, fmt.Sprintf("key not found: %v", err), http.StatusNotFound)
		return
	}

	response := map[string]string{"key": key, "value": value}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *Server) clusterHandler(w http.ResponseWriter, r *http.Request) {
	future := s.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		http.Error(w, fmt.Sprintf("failed to get cluster config: %v", err), http.StatusInternalServerError)
		return
	}

	config := future.Configuration()
	json.NewEncoder(w).Encode(config.Servers)
}

func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	health := map[string]interface{}{
		"node_id":   s.nodeID,
		"state":     s.raft.State().String(),
		"leader":    string(s.raft.Leader()),
		"http_addr": s.httpAddr,
		"raft_addr": s.raftAddr,
		"healthy":   true,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(health)
}
