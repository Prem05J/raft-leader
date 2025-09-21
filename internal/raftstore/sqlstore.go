package raftstore

import (
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	_ "github.com/mattn/go-sqlite3"
)

type SQLiteStore struct {
	db       *sql.DB
	stopChan chan struct{}
	wg       sync.WaitGroup
	mu       sync.RWMutex
}

func NewSQLiteStore(path string, batchSize int, batchTime time.Duration) (*SQLiteStore, error) {
	db, err := sql.Open("sqlite3", path+"?_busy_timeout=10000&_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	// ensure DB is reachable
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("database ping failed: %w", err)
	}

	// Create required tables. Keep table names consistent with the rest of the code.
	// Use separate Exec calls so it's easier to detect which one failed.
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS raft_stable (
			key TEXT PRIMARY KEY,
			value BLOB
		);
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create raft_stable failed: %w", err)
	}

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS raft_log (
			idx INTEGER PRIMARY KEY,
			term INTEGER NOT NULL,
			type INTEGER NOT NULL,
			data BLOB
		);
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create raft_log failed: %w", err)
	}

	// Optional application tables used by writeBatch (keep names consistent):
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS data (
			key TEXT PRIMARY KEY,
			value TEXT
		);
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create data table failed: %w", err)
	}

	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS logs (
			log_index INTEGER PRIMARY KEY,
			key TEXT,
			value TEXT,
			idempotency_key TEXT
		);
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create logs table failed: %w", err)
	}

	// Index for raft_log term lookups
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_raft_log_term ON raft_log(term);`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create index failed: %w", err)
	}

	s := &SQLiteStore{
		db:       db,
		stopChan: make(chan struct{}),
	}
	s.wg.Add(1)
	return s, nil
}

func (s *SQLiteStore) SetUint64(key []byte, val uint64) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, val)
	return s.Set(key, b)
}

func (s *SQLiteStore) Set(key []byte, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec("INSERT OR REPLACE INTO raft_stable(key, value) VALUES (?, ?)", string(key), val)
	return err
}

func (s *SQLiteStore) Get(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var v []byte
	err := s.db.QueryRow("SELECT value FROM raft_stable WHERE key = ?", string(key)).Scan(&v)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (s *SQLiteStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec("DELETE FROM data WHERE key = ?", key)
	return err
}

func (s *SQLiteStore) Snapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query("SELECT key, value FROM data")
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

	return json.Marshal(data)
}

func (s *SQLiteStore) Restore(rc io.ReadCloser) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer rc.Close()

	var data map[string]string
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return err
	}

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Clear existing data
	if _, err := tx.Exec("DELETE FROM data"); err != nil {
		return err
	}

	// Insert new data
	stmt, err := tx.Prepare("INSERT INTO data (key, value) VALUES (?, ?)")
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

func (s *SQLiteStore) Close() error {
	close(s.stopChan)
	s.wg.Wait()
	return s.db.Close()
}

func (s *SQLiteStore) DeleteRange(min, max uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec(`
		DELETE FROM raft_log
		WHERE idx >= ? AND idx <= ?
	`, min, max)
	return err
}

func (s *SQLiteStore) FirstIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var idx sql.NullInt64
	err := s.db.QueryRow("SELECT MIN(idx) FROM raft_log").Scan(&idx)
	if err != nil {
		return 0, err
	}
	if !idx.Valid {
		return 0, nil
	}
	return uint64(idx.Int64), nil
}

func (s *SQLiteStore) LastIndex() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var idx sql.NullInt64
	err := s.db.QueryRow("SELECT MAX(idx) FROM raft_log").Scan(&idx)
	if err != nil {
		return 0, err
	}
	if !idx.Valid {
		return 0, nil
	}
	return uint64(idx.Int64), nil
}

func (s *SQLiteStore) GetLog(index uint64, out *raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var term uint64
	var logType int
	var data []byte

	err := s.db.QueryRow(`
		SELECT term, type, data
		FROM raft_log
		WHERE idx = ?
	`, index).Scan(&term, &logType, &data)

	if err == sql.ErrNoRows {
		return raft.ErrLogNotFound
	}
	if err != nil {
		return err
	}

	out.Index = index
	out.Term = term
	out.Type = raft.LogType(logType)
	out.Data = data
	return nil
}

func (s *SQLiteStore) StoreLog(log *raft.Log) error {
	return s.StoreLogs([]*raft.Log{log})
}

func (s *SQLiteStore) StoreLogs(logs []*raft.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // Safe rollback if not committed

	stmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO raft_log(idx, term, type, data)
		VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, log := range logs {
		_, err := stmt.Exec(log.Index, log.Term, int(log.Type), log.Data)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *SQLiteStore) GetUint64(key []byte) (uint64, error) {
	b, err := s.Get(key)
	if err != nil {
		return 0, err
	}
	if b == nil || len(b) == 0 {
		return 0, nil
	}
	if len(b) != 8 {
		return 0, fmt.Errorf("invalid uint64 bytes length: %d", len(b))
	}
	return binary.BigEndian.Uint64(b), nil
}
