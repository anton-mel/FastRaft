package raft

import (
	"sync"

	"6.824/log"
	"6.824/raft/pb"
)

type KVServer struct {
	rf      *RaftServer           // Raft server handles consensus and log replication
	applyCh chan *pb.LogElement   // Channel to apply committed transactions
	kv      map[int]string        // Key-value store
	saved   map[string]*Persister // Save snapshots
	kvLock  sync.RWMutex          // RW lock for thread-safety
}

func NewKVServer() *KVServer {
	return &KVServer{
		kv:      make(map[int]string),
		applyCh: make(chan *pb.LogElement, 100),
		saved:   make(map[string]*Persister),
	}
}

func (s *KVServer) ApplyOperation(operation string) (string, error) {
	return "", s.rf.PerformOperation(operation) // returns only errors
}

// background service to listen for applyOperation from Raft server
func (s *KVServer) applyTransactionLoop() {
	for {
		txn := <-s.applyCh
		if txn == nil {
			continue
		}
		// received transaction from Raft server, now replicate
		// the operation in KV store (state machine)
		s.applyTransaction(txn)
	}
}

// applyTransaction applies a single transaction to the KV store
func (s *KVServer) applyTransaction(txn *pb.LogElement) {
	log.DPrintf("[%s] applying operation (%s) to KV store", s.rf.Transport.Addr(), txn.Command)

	command := txn.Command
	idx := int(txn.Index)
	s.put(idx, command)
}

func (s *KVServer) put(key int, value string) {
	s.kvLock.Lock()
	defer s.kvLock.Unlock()
	s.kv[key] = value
}

func (s *KVServer) Log_KV() {
	log.DPrintf("[%v] KV map: %v", s.rf.Transport.Addr(), s.kv)
}

func StartKVServer(addr string, peers []string) *KVServer {
	kvServer := NewKVServer()

	// NOTE! Currently this just rewrites the data,
	// so we probably want to handle it more wisely
	kvServer.saved[addr] = MakePersister()

	// So akin to their solution, we might want to check
	// the snapshots, currently if node fails we will not
	// restore the information back (will not keep it).
	kvServer.rf = MakeRaftServer(addr, kvServer.saved[addr], kvServer.applyCh, peers)
	kvServer.rf.StartRaftServer()

	go kvServer.applyTransactionLoop()

	return kvServer
}
