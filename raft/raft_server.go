package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"6.824/log"
	"6.824/raft/pb"

	"6.824/raft/heartbeat"
	"6.824/raft/logfile"

	"golang.org/x/exp/rand"
	"google.golang.org/grpc"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

type RaftServer struct {
	role  int      // Current replicas' role
	peers []string // Ports for of all the peers
	// persister *Persister

	leaderAddr  string
	currentTerm int
	appliedLast int
	commitIdx   int

	nextIdx  map[string]int // only for leaders
	matchIdx map[string]int // only for leaders

	numVotes     int
	votedFor     string
	cWinElection chan struct{}

	Transport Transport
	Heartbeat *heartbeat.Heartbeat
	applyCh   chan *pb.LogElement
	logfile   *logfile.Logfile

	// { address of server, connection client }, we will maintain
	// the connections and reuse them to reduce latency using
	// snapshots (creating new connections increases latency)
	ReplicaConnMap     map[string]*grpc.ClientConn
	ReplicaConnMapLock sync.RWMutex // Lock to protect communication lock
	mu                 sync.Mutex   // Lock to protect this peer's role
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *RaftServer) GetState() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.role == LEADER
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// NOTE: only call when holding lock
// func (rf *RaftServer) persist() {
// 	log.DPrintf("[%v] (persist) Persisting state", rf.Transport.Addr())
// 	w := new(bytes.Buffer)
// 	e := labgob.NewEncoder(w)
// 	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.logfile) != nil {
// 		log.DPrintf("[%v] (persist) Error encoding state", rf.Transport.Addr())
// 		return
// 	}
// 	rf.persister.SaveRaftState(w.Bytes())
// }

// restore previously persisted state.
// NOTE: only call when holding lock
// func (rf *RaftServer) readPersist(data []byte) {
// 	log.DPrintf("[%v] (readPersist) Reading persisted state", rf.Transport.Addr())
// 	if data == nil || len(data) < 1 { // bootstrap without any state?
// 		return
// 	}
// 	r := bytes.NewBuffer(data)
// 	d := labgob.NewDecoder(r)
// 	if d.Decode(&rf.currentTerm) != nil || d.Decode(&rf.votedFor) != nil || d.Decode(&rf.logfile) != nil {
// 		log.DPrintf("[%v] (readPersist) Error decoding state", rf.Transport.Addr())
// 	}
// }

func (s *RaftServer) convertToTransaction(operation string) (*pb.LogElement, error) {
	return &pb.LogElement{
		Index:   int32(s.commitIdx + 1),
		Term:    int32(s.currentTerm),
		Command: operation,
	}, nil
}

// `sendOperationToLeader` is called when an operation reaches a FOLLOWER.
// This function forwards the operation to the LEADER
func sendOperationToLeader(operation string, conn *grpc.ClientConn) error {
	replicateOpsClient := pb.NewReplicateOperationServiceClient(conn)
	_, err := replicateOpsClient.ForwardOperation(
		context.Background(),
		&pb.ForwardOperationRequest{Operation: operation},
	)
	if err != nil {
		return err
	}
	return nil
}

func (rf *RaftServer) PerformOperation(command string) error {
	isLeader := rf.GetState()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.DPrintf("[%v] IF leader: [%v]", rf.Transport.Addr(), isLeader)

	// only the LEADER is allowed to perform the operation
	// and it then replicates that operation across all the nodes.
	// if the current node is not a LEADER, the operation request
	// will be forwarded to the LEADER, who will then perform the operation
	if isLeader {
		txn, err := rf.convertToTransaction(command)
		if err != nil {
			return fmt.Errorf("[%s] error while converting to transaction", rf.Transport.Addr())
		}
		return rf.performTwoPhaseCommit(txn)
	}
	log.DPrintf("[%s] forwarding operation (%s) to leader [%s]", rf.Transport.Addr(), command, rf.leaderAddr)
	rf.ReplicaConnMapLock.RLock()
	defer rf.ReplicaConnMapLock.RUnlock()

	if rf.leaderAddr == "" {
		log.DPrintf("Leader address is not set, cannot forward command <%v>", command)
		return errors.New("leader address is not set")
	}

	// sending operation to the LEADER to perform a TwoPhaseCommit
	return sendOperationToLeader(command, rf.ReplicaConnMap[rf.leaderAddr])
}

// Performs a two phase commit on all the FOLLOWERS
func (rf *RaftServer) performTwoPhaseCommit(txn *pb.LogElement) error {
	// since we are planning to commit commands beyond the node
	// initialization, let's refactor our logfile interface.
	rf.ReplicaConnMapLock.RLock()
	wg := &sync.WaitGroup{}

	// First phase of the TwoPhaseCommit: Commit operation

	// CommitOperation on self
	if _, err := rf.logfile.CommitOperation(rf.commitIdx, rf.commitIdx, txn); err != nil {
		panic(fmt.Errorf("[%s] %v", rf.Transport.Addr(), err))
	}

	log.DPrintf("[%s] performing commit operation on %d followers", rf.Transport.Addr(), len(rf.ReplicaConnMap))

	for addr, conn := range rf.ReplicaConnMap {
		replicateOpsClient := pb.NewReplicateOperationServiceClient(conn)
		log.DPrintf("[%s] sending (CommitOperation: %s) to [%s]", rf.Transport.Addr(), txn.Command, addr)
		response, err := replicateOpsClient.CommitOperation(
			context.Background(),
			&pb.CommitTransaction{
				ExpectedFinalIndex: int64(rf.commitIdx),
				Index:              int64(txn.Index),
				Operation:          txn.Command,
				Term:               int64(txn.Term),
			},
		)
		if err != nil {
			log.DPrintf("[%s] received error in (CommitOperation) from [%s]: %v", rf.Transport.Addr(), addr, err)
			// if there is both, an error and a response, the FOLLOWER is missing
			// some logs. So the LEADER will replicate all the missing logs in the FOLLOWER
			if response != nil {
				wg.Add(1)
				go rf.replicateMissingLogs(int(response.LogfileFinalIndex), addr, replicateOpsClient, wg)
			} else {
				return err
			}
		}
	}

	// wait for all FOLLOWERS to be consistent
	wg.Wait()

	// Second phase of the TwoPhaseCommit: Apply operation

	// ApplyOperation on self
	_, err := rf.logfile.ApplyOperation()
	if err != nil {
		panic(err)
	}

	log.DPrintf("[%s] performing (ApplyOperation) on %d followers", rf.Transport.Addr(), len(rf.ReplicaConnMap))

	rf.commitIdx++ // increment the final commitIndex after applying changes
	// rf.applyCh <- txn

	for _, conn := range rf.ReplicaConnMap {
		replicateOpsClient := pb.NewReplicateOperationServiceClient(conn)
		_, err := replicateOpsClient.ApplyOperation(
			context.Background(),
			&pb.ApplyOperationRequest{},
		)
		if err != nil {
			return err
		}
	}
	rf.ReplicaConnMapLock.RUnlock()

	// rf.persist()
	return nil
}

// `replicateMissingLogs` makes a FOLLOWER consistent with the leader. This is
// called when the FOLLOWER is missing some logs and refuses a commit operation
// request from the LEADER
func (rf *RaftServer) replicateMissingLogs(startIndex int, addr string, client pb.ReplicateOperationServiceClient, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		startIndex++
		txn, err := rf.logfile.GetTransactionWithIndex(startIndex)
		if err != nil {
			log.DPrintf("[%s] error fetching (index: %d) from Logfile", rf.Transport.Addr(), startIndex)
			break
		}
		if txn == nil {
			break
		}
		_, err = client.CommitOperation(
			context.Background(),
			&pb.CommitTransaction{
				ExpectedFinalIndex: int64(startIndex),
				Index:              int64(txn.Index),
				Operation:          txn.Command,
				Term:               int64(txn.Term),
			},
		)
		if err != nil {
			log.DPrintf("[%s] error replicating missing log (index: %d) to [%s]", rf.Transport.Addr(), startIndex, addr)
		}
	}
}

func (rf *RaftServer) sendRequestVote(server string, args *pb.RequestVoteRequest) bool {
	// Acquire the connection to the replica
	rf.ReplicaConnMapLock.RLock()
	conn, ok := rf.ReplicaConnMap[server]
	rf.ReplicaConnMapLock.RUnlock()

	if !ok || conn == nil {
		log.DPrintf("[%v] (sendRequestVote) No connection to server %s", rf.Transport.Addr(), server)
		return false
	}

	// Create a new gRPC client
	raftClient := pb.NewRaftServiceClient(conn)

	// Execute gRPC call
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	grpcReply, err := raftClient.RequestVote(ctx, args)
	if err != nil {
		log.DPrintf("[%v] (sendRequestVote) gRPC call to server %s failed: %v", rf.Transport.Addr(), server, err)
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Process gRPC response
	if rf.currentTerm != int(args.Term) || rf.role != CANDIDATE {
		log.DPrintf("[%v] (sendRequestVote) is not a candidate or behind arg term", rf.Transport.Addr())
		return true
	}
	if grpcReply.Term > int32(rf.currentTerm) {
		log.DPrintf("[%v] (sendRequestVote) Updating my term to %d", rf.Transport.Addr(), grpcReply.Term)
		rf.votedFor = ""
		rf.currentTerm = int(grpcReply.Term)
		rf.role = FOLLOWER
		return true
	}
	if grpcReply.VoteGranted {
		log.DPrintf("[%v] (sendRequestVote) Received vote from %v", rf.Transport.Addr(), server)
		rf.numVotes++
		if rf.numVotes > len(rf.peers)/2 {
			log.DPrintf("[%v] (sendRequestVote) Won election", rf.Transport.Addr())
			// rf.persist()
			lastTxn, _ := rf.logfile.GetFinalTransaction()
			nextIdx := lastTxn.Index + 1
			for _, addr := range rf.peers {
				rf.nextIdx[addr] = int(nextIdx)
			}
			rf.role = LEADER
			rf.leaderAddr = rf.Transport.Addr()
			rf.cWinElection <- struct{}{}
			// if the replica becomes a LEADER, it does not need to listen
			// for heartbeat from other replicas anymore, so stop the
			// heartbeat timeout process
			rf.Heartbeat.Stop()
		}
	}

	return true
}

func (rf *RaftServer) sendAppendEntries(server string, grpcArgs *pb.AppendEntriesRequest, reply *pb.AppendEntriesResponse) {
	log.DPrintf("[%v] (sendAppendEntries) Sending append entries to %v", rf.Transport.Addr(), server)
	// NOTE! I have removed a deep-copying the AppendEntriesArgs

	// Acquire the connection to the replica
	rf.ReplicaConnMapLock.RLock()
	conn, ok := rf.ReplicaConnMap[server]
	rf.ReplicaConnMapLock.RUnlock()

	if !ok || conn == nil {
		log.DPrintf("[%v] (sendAppendEntries) No connection to server %s", rf.Transport.Addr(), server)
		return
	}

	raftClient := pb.NewRaftServiceClient(conn)

	// Execute gRPC call
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	grpcReply, err := raftClient.AppendEntries(ctx, grpcArgs)
	if err != nil {
		log.DPrintf("[%v] (sendAppendEntries) gRPC call to server %s failed: %v", rf.Transport.Addr(), server, err)
		return
	}

	// Update response fields
	reply.Term = int32(grpcReply.Term)
	reply.Success = grpcReply.Success
	reply.NextTryIdx = int32(grpcReply.NextTryIdx)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != int(grpcArgs.Term) || rf.role != LEADER {
		// Log if the role/term has changed
		log.DPrintf("[%v] (sendAppendEntries) Not a leader or term mismatch during RPC to %s", rf.Transport.Addr(), server)
		return
	}

	if rf.currentTerm < int(reply.Term) {
		log.DPrintf("[%v] (sendAppendEntries) Other node has later term %d. Becoming follower ", rf.Transport.Addr(), reply.Term)
		// Update to follower if term mismatch
		rf.currentTerm = int(reply.Term)
		rf.role = FOLLOWER
		rf.votedFor = ""
		// rf.persist()
		return
	}
	if reply.Success {
		// Successful log replication
		if len(grpcArgs.Entries) > 0 {
			rf.matchIdx[server] = int(grpcArgs.Entries[len(grpcArgs.Entries)-1].Index)
			rf.nextIdx[server] = rf.matchIdx[server] + 1
		}
	} else {
		// Adjust next index for failed replication
		rf.nextIdx[server] = int(reply.NextTryIdx)
	}

	// Update commit index based on quorum
	last_log, err := rf.logfile.GetFinalTransaction()
	if err != nil {
		log.DPrintf("[%v] (sendAppendEntries) Error retrieving final transaction: %v", rf.Transport.Addr(), err)
		return
	}
	for i := int(last_log.Index); i > rf.commitIdx; i-- {
		count := 1
		for _, match := range rf.matchIdx {
			if match >= i {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			rf.commitIdx = i
			go rf.applyCommittedEntries()
			break
		}
	}
}

// Helper to apply committed entries
func (rf *RaftServer) applyCommittedEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Get the base transaction
	baseTxn, err := rf.logfile.GetTransactionWithIndex(0)
	if err != nil {
		log.DPrintf("[%v] (applyCommittedEntries) Error retrieving base transaction: %v", rf.Transport.Addr(), err)
		return
	}

	for i := rf.appliedLast + 1; i <= rf.commitIdx; i++ {
		// Get i-th transaction to Apply the Message to a SM
		_, err := rf.logfile.GetTransactionWithIndex(i - int(baseTxn.Index))
		if err != nil {
			log.DPrintf("[%v] (applyCommittedEntries) Error retrieving transaction at index %d: %v", rf.Transport.Addr(), i, err)
			continue
		}

		// Send the committed transaction
		// to the apply channel
		// rf.applyCh <- txn
	}

	// Update the last applied index
	rf.appliedLast = rf.commitIdx
}

// `broadcastHeartbeat` is called by the leader to
// send a heartbeat to followers every second
func (rf *RaftServer) broadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.DPrintf("[%s] sending heartbeat to %d followers", rf.Transport.Addr(), len(rf.ReplicaConnMap))
	baseIdx := 0

	// Get the base index of the log
	if txn, err := rf.logfile.GetFinalTransaction(); err == nil && txn != nil {
		baseIdx = int(txn.Index)
	}

	for _, peerAddr := range rf.peers {
		if rf.role == LEADER && peerAddr != rf.Transport.Addr() {
			// Get the next index for this peer
			nextIdx, exists := rf.nextIdx[peerAddr]
			if !exists {
				log.DPrintf("[%v] (broadcastHeartbeat) nextIdx map does not include [%v] index", rf.Transport.Addr(), peerAddr)
				return
			}

			if nextIdx <= baseIdx {
				// snapshot? but not necessary for lab
				// continue
			} else {
				args := pb.AppendEntriesRequest{
					Term:         int32(rf.currentTerm),
					Leader:       rf.Transport.Addr(),
					PrevLogIdx:   int32(nextIdx - 1),
					LeaderCommit: int32(rf.commitIdx),
					LeaderAddr:   rf.Transport.Addr(),
				}

				// Set the previous log term if applicable
				if int(args.PrevLogIdx) >= baseIdx {
					if txn, err := rf.logfile.GetTransactionWithIndex(int(args.PrevLogIdx) - baseIdx); err == nil {
						args.PrevLogTerm = int32(txn.Term)
					}
				}

				// Append log entries if there are new entries
				if nextIdx <= rf.logfile.Size() {
					entries := make([]*pb.LogElement, 0)
					for idx := nextIdx - baseIdx; idx < rf.logfile.Size(); idx++ {
						if txn, err := rf.logfile.GetTransactionWithIndex(idx); err == nil {
							entries = append(entries, txn)
						}
					}
					args.Entries = entries
				}

				// Send AppendEntries RPC in a separate goroutine
				go rf.sendAppendEntries(peerAddr, &args, &pb.AppendEntriesResponse{})
			}
		}
	}
}

// The ticker go routine starts a new election
// if this peer hasn't received heartsbeats recently
func (rf *RaftServer) ticker() {
	for { // state switch process
		rf.mu.Lock()
		switch rf.role {
		case FOLLOWER:
			rf.mu.Unlock()
			// start the heartbeat timeout
			// only if not already set
			if rf.Heartbeat == nil {
				log.DPrintf("[%s] FOLLOWER: Starting timeout", rf.Transport.Addr())
				// this is not needed, since we are anyways working in the real environment
				timeoutHeartbeat := time.Duration(rand.Intn(200)+1100) * time.Millisecond
				rf.Heartbeat = heartbeat.NewHeartbeat(timeoutHeartbeat, func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					log.DPrintf("[%s] FOLLOWER timeout expired, becoming CANDIDATE", rf.Transport.Addr())
					rf.role = CANDIDATE
					// rf.persist()
				})
			} else if rf.Heartbeat.Expired() {
				// if the timout is stopped,
				// restart (from LEADER/CANDIDATE)
				rf.Heartbeat.Beat()
			}

		case CANDIDATE:
			// Start new election
			rf.currentTerm++
			log.DPrintf("[%s] CANDIDATE: Starting election for term %d", rf.Transport.Addr(), rf.currentTerm)
			rf.votedFor = rf.Transport.Addr()
			rf.numVotes = 1
			// rf.persist()
			rf.mu.Unlock()

			go rf.startElection()

			select {
			case <-rf.cWinElection:
				log.DPrintf("[%v] Became Leader", rf.Transport.Addr())
				rf.mu.Lock()
				rf.role = LEADER
				rf.mu.Unlock()
			case <-time.After(time.Duration(rand.Intn(200)+600) * time.Millisecond):
				log.DPrintf("[%v] Became Follower", rf.Transport.Addr())
				rf.mu.Lock()
				rf.role = FOLLOWER
				rf.mu.Unlock()
			}

		case LEADER:
			rf.mu.Unlock()
			// the LEADER will send heartbeat to the FOLLOWERS
			go rf.broadcastHeartbeat()
			// rf.persist()

			// limited to 10 heartbeats per second
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// `startElection` sends RequestVote RPCs to peers.
func (rf *RaftServer) startElection() {
	rf.mu.Lock()

	lastLog, _ := rf.logfile.GetFinalTransaction()
	args := pb.RequestVoteRequest{
		Term:          int32(rf.currentTerm),
		CandidatePort: rf.Transport.Addr(),
		LastLogIdx:    lastLog.Index,
		LastLogTerm:   lastLog.Term,
	}
	rf.mu.Unlock()

	for _, addr := range rf.peers {
		if addr != rf.Transport.Addr() {
			go func(addr string) {
				rf.sendRequestVote(addr, &args)
			}(addr)
		}
	}
}

// Sends requests to other replicas so that they can add this
// server to their replicaConnMap (establish gRPC connection)
func (rf *RaftServer) bootstrapNetwork() {
	wg := &sync.WaitGroup{}
	for _, addr := range rf.peers {
		wg.Add(1)
		if len(addr) == 0 {
			continue
		}
		go func(s *RaftServer, addr string, wg *sync.WaitGroup) {
			log.DPrintf("[%s] attempting to connect with [%s]", rf.Transport.Addr(), addr)
			if err := rf.Transport.Dial(s, addr); err != nil {
				log.DPrintf("[%s]: dial error while connecting to [%s]: %v", rf.Transport.Addr(), addr, err)
			}
			wg.Done()
		}(rf, addr, wg)
	}
	wg.Wait()
	log.DPrintf("[%s] bootstrapping completed", rf.Transport.Addr())
}

func MakeRaftServer(me string, applyCh chan *pb.LogElement, peers []string) *RaftServer {
	rf := &RaftServer{}

	rf.mu.Lock()
	rf.role = FOLLOWER
	rf.peers = peers
	// rf.persister = persister

	rf.currentTerm = 0
	rf.appliedLast = 0
	rf.commitIdx = 0

	rf.nextIdx = make(map[string]int)
	rf.matchIdx = make(map[string]int)

	rf.numVotes = 0
	rf.votedFor = ""
	rf.cWinElection = make(chan struct{}, 1000)

	rf.Transport = &GRPCTransport{ListenAddr: me}
	rf.applyCh = applyCh
	rf.logfile = logfile.NewLogfile()
	rf.ReplicaConnMap = make(map[string]*grpc.ClientConn)

	// rf.readPersist(persister.ReadRaftState())
	// rf.persist()
	rf.mu.Unlock()

	return rf
}

func (rf *RaftServer) StartRaftServer() error {
	// start function should not apply the command on launch
	// bur rather let's create a separate performOperation
	// function to send as many requests as needed, which
	// provides more flexibility in unit testing Raft
	go rf.startGrpcServer()
	time.Sleep(time.Second * 5) // wait for server to start

	log.DPrintf("[%s] Raft Server Started 🚀", rf.Transport.Addr())

	// send request to bootstrapped servers to
	// add this to replica to their `replicaConnMap`
	rf.bootstrapNetwork()

	// start elections after
	// all grpc are setted up
	go rf.ticker()

	return nil
}

// SetUp the gRPC servers
func (rf *RaftServer) startGrpcServer() error {
	lis, err := net.Listen("tcp", rf.Transport.Addr())
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %v", rf.Transport.Addr(), err)
	}

	grpcServer := grpc.NewServer()
	// register one gRPC server with services per each node.
	// keep the connections map `ReplicaConnMap` for faster restart
	// by saving it to the snapshot and fetching when needed (stretch goal)
	pb.RegisterBootstrapServiceServer(grpcServer, NewBootstrapServiceServer(rf))             // replicate the grpc connection around the peers
	pb.RegisterRaftServiceServer(grpcServer, NewRaftServiceServer(rf))                       // election processes and transaction management
	pb.RegisterHeartbeatServiceServer(grpcServer, NewHeartbeatServiceServer(rf))             // heartbeat timout handling (issue: channel handling)
	pb.RegisterReplicateOperationServiceServer(grpcServer, NewReplicateOpsServiceServer(rf)) // leader log commit handling from non-leader nodes
	pb.RegisterLoadDriverServiceServer(grpcServer, NewLoadDriverServiceServer(rf))           // handle testing with external pod manager

	if err = grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve gRPC on port %s: %v", rf.Transport.Addr(), err)
	}
	return nil
}
