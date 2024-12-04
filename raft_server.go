package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"6.824/heartbeat"
	"6.824/logfile"
	"6.824/pb"
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

	leaderAddr  string
	currentTerm int
	appliedLast int
	commitIdx   int

	nextIdx  map[string]int // only for leaders
	matchIdx map[string]int // only for leaders

	numVotes     int
	votedFor     string
	cWinElection chan struct{}

	transport Transport            // gRPC to join replicas' list
	Heartbeat *heartbeat.Heartbeat // gRPC for leader connection
	applyCh   chan *pb.LogElement  // ch to apply state machine updates
	logfile   *logfile.Logfile     // array of LogEntries

	// { address of server, connection client }, we will maintain
	// the connections and reuse them to reduce latency using
	// snapshots (creating new connections increases latency)
	ReplicaConnMap     map[string]*grpc.ClientConn
	ReplicaConnMapLock sync.RWMutex // Lock to protect communication lock
	mu                 sync.Mutex   // Lock to protect this peer's role
}

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

// Performs a two phase commit on all the FOLLOWERS
func (rf *RaftServer) performTwoPhaseCommit(txn *pb.LogElement) error {
	// since we are planning to commit commands beyond the node
	// initialization, let's refactor our logfile interface.
	rf.ReplicaConnMapLock.RLock()
	wg := &sync.WaitGroup{}

	// First phase of the TwoPhaseCommit: Commit operation

	// CommitOperation on self
	if _, err := rf.logfile.CommitOperation(rf.commitIdx, rf.commitIdx, txn); err != nil {
		panic(fmt.Errorf("[%s] %v", rf.transport.Addr(), err))
	}

	log.Printf("[%s] performing commit operation on %d followers\n", rf.transport.Addr(), len(rf.ReplicaConnMap))

	for addr, conn := range rf.ReplicaConnMap {
		replicateOpsClient := pb.NewReplicateOperationServiceClient(conn)
		log.Printf("[%s] sending (CommitOperation: %s) to [%s]\n", rf.transport.Addr(), txn.Command, addr)
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
			log.Printf("[%s] received error in (CommitOperation) from [%s]: %v", rf.transport.Addr(), addr, err)
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

	log.Printf("[%s] performing (ApplyOperation) on %d followers\n", rf.transport.Addr(), len(rf.ReplicaConnMap))

	rf.commitIdx++ // increment the final commitIndex after applying changes
	rf.applyCh <- txn

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
			log.Printf("[%s] error fetching (index: %d) from Logfile\n", rf.transport.Addr(), startIndex)
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
			log.Printf("[%s] error replicating missing log (index: %d) to [%s]\n", rf.transport.Addr(), startIndex, addr)
		}
	}
}

func (rf *RaftServer) sendRequestVote(server string, args *pb.RequestVoteRequest) bool {
	// Acquire the connection to the replica
	rf.ReplicaConnMapLock.RLock()
	conn, ok := rf.ReplicaConnMap[server]
	rf.ReplicaConnMapLock.RUnlock()

	if !ok || conn == nil {
		log.Printf("(sendRequestVote) [%v] No connection to server %s", rf.transport.Addr(), server)
		return false
	}

	// Create a new gRPC client
	raftClient := pb.NewRaftServiceClient(conn)

	// Execute gRPC call
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	grpcReply, err := raftClient.RequestVote(ctx, args)
	if err != nil {
		log.Printf("(sendRequestVote) [%v] gRPC call to server %s failed: %v", rf.transport.Addr(), server, err)
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Process gRPC response
	if rf.currentTerm != int(args.Term) || rf.role != CANDIDATE {
		log.Printf("(sendRequestVote) [%v] is not a candidate or behind arg term", rf.transport.Addr())
		return true
	}
	if grpcReply.Term > int32(rf.currentTerm) {
		log.Printf("(sendRequestVote) [%v] Updating my term to %d", rf.transport.Addr(), grpcReply.Term)
		rf.votedFor = ""
		rf.currentTerm = int(grpcReply.Term)
		rf.role = FOLLOWER
		return true
	}
	if grpcReply.VoteGranted {
		log.Printf("(sendRequestVote) [%v] Received vote from %v", rf.transport.Addr(), server)
		rf.numVotes++
		if rf.numVotes > len(rf.peers)/2 {
			log.Printf("(sendRequestVote) [%v] Won election", rf.transport.Addr())
			lastTxn, _ := rf.logfile.GetFinalTransaction()
			nextIdx := lastTxn.Index + 1
			for _, addr := range rf.peers {
				rf.nextIdx[addr] = int(nextIdx)
			}
			rf.role = LEADER
			rf.leaderAddr = rf.transport.Addr()
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
	log.Printf("(sendAppendEntries) [%v] Sending append entries to %v", rf.transport.Addr(), server)
	// NOTE! I have removed a deep-copying the AppendEntriesArgs

	// Acquire the connection to the replica
	rf.ReplicaConnMapLock.RLock()
	conn, ok := rf.ReplicaConnMap[server]
	rf.ReplicaConnMapLock.RUnlock()

	if !ok || conn == nil {
		log.Printf("(sendAppendEntries) [%v] No connection to server %s", rf.transport.Addr(), server)
		return
	}

	raftClient := pb.NewRaftServiceClient(conn)

	// Execute gRPC call
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	grpcReply, err := raftClient.AppendEntries(ctx, grpcArgs)
	if err != nil {
		log.Printf("(sendAppendEntries) [%v] gRPC call to server %s failed: %v", rf.transport.Addr(), server, err)
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
		log.Printf("(sendAppendEntries) [%v] Not a leader or term mismatch during RPC to %s", rf.transport.Addr(), server)
		return
	}

	if rf.currentTerm < int(reply.Term) {
		log.Printf("(sendAppendEntries) [%v] Other node has later term %d. Becoming follower ", rf.transport.Addr(), reply.Term)
		// Update to follower if term mismatch
		rf.currentTerm = int(reply.Term)
		rf.role = FOLLOWER
		rf.votedFor = ""
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
		log.Printf("(sendAppendEntries) Error retrieving final transaction: %v", err)
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
		log.Printf("(applyCommittedEntries) Error retrieving base transaction: %v", err)
		return
	}

	for i := rf.appliedLast + 1; i <= rf.commitIdx; i++ {
		// Get i-th transaction to Apply the Message to a SM
		txn, err := rf.logfile.GetTransactionWithIndex(i - int(baseTxn.Index))
		if err != nil {
			log.Printf("(applyCommittedEntries) Error retrieving transaction at index %d: %v", i, err)
			continue
		}

		// Send the committed transaction
		// to the apply channel
		rf.applyCh <- txn
	}

	// Update the last applied index
	rf.appliedLast = rf.commitIdx
}

// `broadcastHeartbeat` is called by the leader to
// send a heartbeat to followers every second
func (rf *RaftServer) broadcastHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("[%s] sending heartbeat to %d followers\n", rf.transport.Addr(), len(rf.ReplicaConnMap))
	baseIdx := 0

	// Get the base index of the log
	if txn, err := rf.logfile.GetFinalTransaction(); err == nil && txn != nil {
		baseIdx = int(txn.Index)
	}

	for _, peerAddr := range rf.peers {
		if rf.role == LEADER && peerAddr != rf.transport.Addr() {
			// Get the next index for this peer
			nextIdx, exists := rf.nextIdx[peerAddr]
			if !exists {
				// DEBUG: Theoretically this should not happen
				panic(fmt.Errorf("(broadcastHeartbeat) nextIdx map does not include [%v] index", peerAddr))
			}

			if nextIdx <= baseIdx {
				// snapshot? but not necessary for lab
				// continue
			} else {
				args := pb.AppendEntriesRequest{
					Term:         int32(rf.currentTerm),
					Leader:       rf.transport.Addr(),
					PrevLogIdx:   int32(nextIdx - 1),
					LeaderCommit: int32(rf.commitIdx),
					LeaderAddr:   rf.transport.Addr(),
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
				log.Printf("[%s] FOLLOWER: Starting timeout\n", rf.transport.Addr())
				// this is not needed, since we are anyways working in the real environment
				timeoutHeartbeat := time.Duration(rand.Intn(200)+1100) * time.Millisecond
				rf.Heartbeat = heartbeat.NewHeartbeat(timeoutHeartbeat, func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					log.Printf("[%s] FOLLOWER timeout expired, becoming CANDIDATE\n", rf.transport.Addr())
					rf.role = CANDIDATE
				})
			} else if rf.Heartbeat.Expired() {
				// if the timout is stopped,
				// restart (from LEADER/CANDIDATE)
				rf.Heartbeat.Beat()
			}

		case CANDIDATE:
			// Start new election
			rf.currentTerm++
			log.Printf("[%s] CANDIDATE: Starting election for term %d\n", rf.transport.Addr(), rf.currentTerm)
			rf.votedFor = rf.transport.Addr()
			rf.numVotes = 1

			rf.mu.Unlock()
			go rf.startElection()

			select {
			case <-rf.cWinElection:
				log.Printf("[%v] Became Leader", rf.transport.Addr())
				rf.mu.Lock()
				rf.role = LEADER
				rf.mu.Unlock()
			case <-time.After(time.Duration(rand.Intn(200)+600) * time.Millisecond):
				log.Printf("[%v] Became Follower", rf.transport.Addr())
				rf.mu.Lock()
				rf.role = FOLLOWER
				rf.mu.Unlock()
			}

		case LEADER:
			rf.mu.Unlock()
			// the LEADER will send heartbeat to the FOLLOWERS
			go rf.broadcastHeartbeat()
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
		CandidatePort: rf.transport.Addr(),
		LastLogIdx:    lastLog.Index,
		LastLogTerm:   lastLog.Term,
	}
	rf.mu.Unlock()

	for _, addr := range rf.peers {
		if addr != rf.transport.Addr() {
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
			log.Printf("[%s] attempting to connect with [%s]\n", rf.transport.Addr(), addr)
			if err := rf.transport.Dial(s, addr); err != nil {
				log.Printf("[%s]: dial error while connecting to [%s]: %v\n", rf.transport.Addr(), addr, err)
			}
			wg.Done()
		}(rf, addr, wg)
	}
	wg.Wait()
	log.Printf("[%s] bootstrapping completed\n", rf.transport.Addr())
}

func MakeRaftServer(me string, applyCh chan *pb.LogElement, peers []string) *RaftServer {
	rf := &RaftServer{}

	rf.mu.Lock()
	rf.role = FOLLOWER
	rf.peers = peers

	rf.currentTerm = 0
	rf.appliedLast = 0
	rf.commitIdx = 0

	rf.nextIdx = make(map[string]int)
	rf.matchIdx = make(map[string]int)

	rf.numVotes = 0
	rf.votedFor = ""
	rf.cWinElection = make(chan struct{}, 1000)

	rf.transport = &GRPCTransport{ListenAddr: me}
	rf.applyCh = applyCh
	rf.logfile = logfile.NewLogfile()
	rf.ReplicaConnMap = make(map[string]*grpc.ClientConn)
	rf.mu.Unlock()

	return rf
}

func (rf *RaftServer) StartRaftServer() error {
	// start function should not apply the command on launch
	// bur rather let's create a separate performOperation
	// function to send as many requests as needed, which
	// provides more flexibility in unit testing Raft
	go rf.startGrpcServer()
	time.Sleep(time.Second * 3) // wait for server to start

	log.Printf("%s\n", separator)
	log.Printf("   🚀 Raft Server Started 🚀   \n")
	log.Printf("       Address: [%s]         \n", rf.transport.Addr())
	log.Printf("%s\n", separator)

	// send request to bootstrapped servers to
	// add this to replica to their `replicaConnMap`
	rf.bootstrapNetwork()

	// start elections after
	// all grpc are setted up
	go rf.ticker()

	return nil
}

func (rf *RaftServer) PerformOperation(command string) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.role == LEADER
	log.Printf("[%v] IF leader: [%v]\n", rf.transport.Addr(), isLeader)

	// only the LEADER is allowed to perform the operation
	// and it then replicates that operation across all the nodes.
	// if the current node is not a LEADER, the operation request
	// will be forwarded to the LEADER, who will then perform the operation
	if isLeader {
		txn, err := rf.convertToTransaction(command)
		if err != nil {
			return fmt.Errorf("[%s] error while converting to transaction", rf.transport.Addr())
		}
		return rf.performTwoPhaseCommit(txn)
	}
	log.Printf("[%s] forwarding operation (%s) to leader [%s]\n", rf.transport.Addr(), command, rf.leaderAddr)
	rf.ReplicaConnMapLock.RLock()
	defer rf.ReplicaConnMapLock.RUnlock()

	if rf.leaderAddr == "" {
		log.Printf("Leader address is not set, cannot forward command <%v>", command)
		return errors.New("leader address is not set")
	}
	// FIX: THIS IS NOT WORKING YET (SO CAN ONLY APPLY ON LEADER)
	// sending operation to the LEADER to perform a TwoPhaseCommit
	return sendOperationToLeader(command, rf.ReplicaConnMap[rf.leaderAddr])
}

// SetUp the gRPC servers
func (rf *RaftServer) startGrpcServer() error {
	lis, err := net.Listen("tcp", rf.transport.Addr())
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %v", rf.transport.Addr(), err)
	}

	grpcServer := grpc.NewServer()
	// register one gRPC server with services per each node.
	// keep the connections map `ReplicaConnMap` for faster restart
	// by saving it to the snapshot and fetching when needed (stretch goal)
	pb.RegisterBootstrapServiceServer(grpcServer, NewBootstrapServiceServer(rf))             // replicate the grpc connection around the peers
	pb.RegisterRaftServiceServer(grpcServer, NewRaftServiceServer(rf))                       // election processes and transaction management
	pb.RegisterHeartbeatServiceServer(grpcServer, NewHeartbeatServiceServer(rf))             // heartbeat timout handling (issue: channel handling)
	pb.RegisterReplicateOperationServiceServer(grpcServer, NewReplicateOpsServiceServer(rf)) // leader log commit handling from non-leader nodes

	if err = grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve gRPC on port %s: %v", rf.transport.Addr(), err)
	}
	return nil
}
