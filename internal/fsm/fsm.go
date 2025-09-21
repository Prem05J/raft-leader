package fsm

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
	_ "github.com/mattn/go-sqlite3"
)

type batchPayload struct {
	Entries [][]byte
}

type FSM struct {
	db    *sql.DB
	mu    sync.RWMutex
	dedup *DedupCache
}

func NewFSM(dbPath string) (*FSM, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS kv_store (
			key TEXT PRIMARY KEY,
			value TEXT
		)
	`)
	if err != nil {
		return nil, err
	}

	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS client_requests (
			idempotency_key TEXT PRIMARY KEY,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return nil, err
	}

	dedup := NewDedupCache(1000)
	return &FSM{db: db, dedup: dedup}, nil
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Try to process as batch first
	batch, err := f.deserializeBatch(l.Data)
	if err == nil {
		return f.processBatch(batch)
	}

	// Fallback to single command
	return f.processSingleCommand(l.Data)
}

func (f *FSM) deserializeBatch(data []byte) (batchPayload, error) {
	var batch batchPayload
	buf := bytes.NewReader(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&batch); err != nil {
		return batchPayload{}, fmt.Errorf("gob decode failed: %w", err)
	}
	return batch, nil
}

func (f *FSM) processBatch(batch batchPayload) error {
	tx, err := f.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, entryData := range batch.Entries {
		if err := f.processCommandInTransaction(tx, entryData); err != nil {
			log.Printf("Error processing command in batch: %v", err)
		}
	}

	return tx.Commit()
}

func (f *FSM) processSingleCommand(data []byte) error {
	tx, err := f.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := f.processCommandInTransaction(tx, data); err != nil {
		return err
	}

	return tx.Commit()
}

func (f *FSM) processCommandInTransaction(tx *sql.Tx, data []byte) error {
	// Try JSON format first
	var cmd map[string]string
	if err := json.Unmarshal(data, &cmd); err != nil {
		return f.processStringCommandInTransaction(tx, string(data))
	}

	return f.processJSONCommandInTransaction(tx, cmd)
}

func (f *FSM) processJSONCommandInTransaction(tx *sql.Tx, cmd map[string]string) error {
	op, ok := cmd["op"]
	if !ok {
		return fmt.Errorf("missing operation in command")
	}

	key, ok := cmd["key"]
	if !ok {
		return fmt.Errorf("missing key in command")
	}

	// Generate idempotency key
	cmdData, err := json.Marshal(cmd)
	if err != nil {
		return fmt.Errorf("failed to marshal command: %w", err)
	}

	hash := sha256.Sum256(cmdData)
	hashStr := hex.EncodeToString(hash[:])

	// Check idempotency
	if f.dedup.Seen(hashStr) {
		return nil
	}

	var exists int
	err = tx.QueryRow("SELECT 1 FROM client_requests WHERE idempotency_key = ?", hashStr).Scan(&exists)
	if err == nil {
		f.dedup.Add(hashStr)
		return nil
	}

	_, err = tx.Exec("INSERT INTO client_requests (idempotency_key) VALUES (?)", hashStr)
	if err != nil {
		return fmt.Errorf("failed to insert idempotency key: %w", err)
	}

	f.dedup.Add(hashStr)

	// Execute operation
	switch strings.ToUpper(op) {
	case "SET":
		value, ok := cmd["value"]
		if !ok {
			return fmt.Errorf("missing value in SET command")
		}

		_, err := tx.Exec(`
			INSERT INTO kv_store (key, value) 
			VALUES (?, ?) 
			ON CONFLICT(key) DO UPDATE SET value = excluded.value
		`, key, value)

		if err != nil {
			return fmt.Errorf("set failed: %w", err)
		}

	case "DELETE":
		_, err := tx.Exec("DELETE FROM kv_store WHERE key = ?", key)
		if err != nil {
			return fmt.Errorf("delete failed: %w", err)
		}

	default:
		return fmt.Errorf("unknown operation: %s", op)
	}

	return nil
}

func (f *FSM) processStringCommandInTransaction(tx *sql.Tx, cmdStr string) error {
	parts := strings.SplitN(cmdStr, " ", 3)
	if len(parts) < 2 {
		return fmt.Errorf("invalid command: %s", cmdStr)
	}

	operation := parts[0]
	key := parts[1]
	value := ""
	if len(parts) > 2 {
		value = parts[2]
	}

	cmd := map[string]string{
		"op":  operation,
		"key": key,
	}
	if operation == "SET" {
		cmd["value"] = value
	}

	return f.processJSONCommandInTransaction(tx, cmd)
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	rows, err := f.db.Query("SELECT key, value FROM kv_store")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	data := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		data[key] = value
	}

	return &Snapshot{data: data}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer rc.Close()

	_, err := f.db.Exec("DELETE FROM kv_store")
	if err != nil {
		return err
	}

	var data map[string]string
	decoder := json.NewDecoder(rc)
	if err := decoder.Decode(&data); err != nil {
		return err
	}

	tx, err := f.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	stmt, err := tx.Prepare("INSERT INTO kv_store (key, value) VALUES (?, ?)")
	if err != nil {
		return err
	}
	defer stmt.Close()

	for key, value := range data {
		if _, err := stmt.Exec(key, value); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (f *FSM) Get(key string) (string, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	var value string
	err := f.db.QueryRow("SELECT value FROM kv_store WHERE key = ?", key).Scan(&value)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", fmt.Errorf("key not found: %s", key)
		}
		return "", err
	}
	return value, nil
}

type Snapshot struct {
	data map[string]string
}

func (s *Snapshot) Persist(sink raft.SnapshotSink) error {
	encoder := json.NewEncoder(sink)
	if err := encoder.Encode(s.data); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *Snapshot) Release() {
	// No resources to release
}
