package main

import (
	"log"
	"sync"

	"6.824/logfile"
)

type KVServer struct {
	rf      *RaftServer              // Raft server handles consensus and log replication
	applyCh chan *logfile.LogElement // Channel to apply committed transactions
	kv      map[int]string           // Key-value store
	kvLock  sync.RWMutex             // RW lock for thread-safety
}

func NewKVServer() *KVServer {
	return &KVServer{
		kv:      make(map[int]string),
		applyCh: make(chan *logfile.LogElement, 100),
	}
}

func (s *KVServer) ApplyOperation(operation string) (string, error) {
	return "", s.rf.PerformOperation(operation)
}

// background service to listen for applyOperation from Raft server
func (s *KVServer) applyTransactionLoop() {
	for {
		txn := <-s.applyCh
		if txn == nil {
			continue
		}
		// received transaction from Raft server, now replicate
		// the operation in KV store
		s.applyTransaction(txn)
	}
}

// applyTransaction applies a single transaction to the KV store
func (s *KVServer) applyTransaction(txn *logfile.LogElement) {
	log.Printf("[%s] applying operation (%s) to KV store\n", s.rf.transport.Addr(), txn.Command)

	command := txn.Command
	idx := txn.Index
	s.put(idx, command)
}

func (s *KVServer) put(key int, value string) {
	s.kvLock.Lock()
	defer s.kvLock.Unlock()
	s.kv[key] = value
}

func StartKVServer(addr string, peers []string) *KVServer {
	kvServer := NewKVServer()

	// So akin to their solution, we might want to check
	// the snapshots, currently if node fails we will not
	// restore the information back (will not keep it).
	kvServer.rf = MakeRaftServer(addr, kvServer.applyCh, peers)
	kvServer.rf.Start()

	go kvServer.applyTransactionLoop()

	return kvServer
}
