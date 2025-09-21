package batcher

import (
	"bytes"
	"encoding/gob"
	"log"
	"time"

	"github.com/hashicorp/raft"
)

// logEntry represents a single client request
type logEntry struct {
	Data []byte
}

// batchPayload represents a batch of entries submitted as one Raft log
type batchPayload struct {
	Entries [][]byte
}

// Batcher implements high-throughput Raft batching
type Batcher struct {
	raft         *raft.Raft
	inputCh      chan *logEntry
	batch        []*logEntry
	batchSize    int
	batchTimeout time.Duration
	shutdownCh   chan struct{}
}

func NewBatcher(r *raft.Raft, batchSize int, timeout time.Duration) *Batcher {
	b := &Batcher{
		raft:         r,
		inputCh:      make(chan *logEntry, 10000),
		batch:        make([]*logEntry, 0, batchSize),
		batchSize:    batchSize,
		batchTimeout: timeout,
		shutdownCh:   make(chan struct{}),
	}
	go b.run()
	return b
}

// Apply is the public API for clients to submit an entry.
func (b *Batcher) Apply(data []byte) error {
	entry := &logEntry{Data: data}
	b.inputCh <- entry
	return nil
}

// run batches incoming requests and flushes by size or timeout
func (b *Batcher) run() {
	ticker := time.NewTicker(b.batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case entry := <-b.inputCh:
			b.batch = append(b.batch, entry)
			if len(b.batch) >= b.batchSize {
				b.SendBatch()
			}
		case <-ticker.C:
			if len(b.batch) > 0 {
				b.SendBatch()
			}
		case <-b.shutdownCh:
			if len(b.batch) > 0 {
				b.SendBatch()
			}
			return
		}
	}
}

// SendBatch serializes the batch and submits it as one Raft log entry
func (b *Batcher) SendBatch() {
	if len(b.batch) == 0 {
		return
	}

	var entries [][]byte
	for _, e := range b.batch {
		entries = append(entries, e.Data)
	}

	payload := batchPayload{Entries: entries}

	batchData, err := serializeBatch(payload)
	if err != nil {
		log.Printf("Failed to serialize batch: %v", err)
		b.batch = b.batch[:0]
		return
	}

	f := b.raft.Apply(batchData, 5*time.Second)
	if err := f.Error(); err != nil {
		log.Printf("Failed to apply batch: %v", err)
	}

	b.batch = make([]*logEntry, 0, b.batchSize)
}

// Stop cleanly shuts down the batcher
func (b *Batcher) Stop() {
	close(b.shutdownCh)
}

// serializeBatch encodes a batchPayload into bytes using gob
func serializeBatch(p batchPayload) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(p); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
