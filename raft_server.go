package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"6.824/heartbeat"
	"6.824/logfile"
	"6.824/pb"

	"google.golang.org/grpc"
)

const NotVoted string = "NONE"

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

const (
	// You may want to increase the timout period
	// to skip the election process
	HEARTBEAT_PERIOD  = 100 * time.Millisecond
	HEARTBEAT_TIMEOUT = 1000 * time.Millisecond
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

// Performs the operation requested by the client.
func (rf *RaftServer) PerformOperation(operation string) error {
	// only the LEADER is allowed to perform the operation
	// and it then replicates that operation across all the nodes.
	// if the current node is not a LEADER, the operation request
	// will be forwarded to the LEADER, who will then perform the operation
	log.Printf("[%s] received operation (%s)\n", rf.transport.Addr(), operation)
	if rf.role == LEADER {
		txn, err := rf.convertToTransaction(operation)
		if err != nil {
			return fmt.Errorf("[%s] error while converting to transaction", rf.transport.Addr())
		}
		return rf.performTwoPhaseCommit(txn)
	}
	log.Printf("[%s] forwarding operation (%s) to leader [%s]\n", rf.transport.Addr(), operation, rf.leaderAddr)
	rf.ReplicaConnMapLock.RLock()
	defer rf.ReplicaConnMapLock.RUnlock()

	// sending operation to the LEADER to perform a TwoPhaseCommit
	return sendOperationToLeader(operation, rf.ReplicaConnMap[rf.leaderAddr])
}

func (s *RaftServer) convertToTransaction(operation string) (*pb.LogElement, error) {
	return &pb.LogElement{
		Index:   int32(s.commitIdx + 1),
		Command: operation,
		Term:    int32(s.currentTerm),
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

// func (rf *RaftServer) GetState() (int, bool) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	return rf.currentTerm, (rf.role == LEADER)
// }

// type RequestVoteArgs struct {
// 	Term         int
// 	CandidateId  string
// 	LastLogIndex int
// 	LastLogTerm  int
// }

// type RequestVoteReply struct {
// 	Term        int
// 	VoteGranted bool
// }

// func (rf *RaftServer) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	if rf.currentTerm > args.Term {
// 		reply.VoteGranted = false
// 		reply.Term = rf.currentTerm
// 		return
// 	}
// 	if rf.currentTerm < args.Term {
// 		rf.currentTerm = args.Term
// 		rf.votedFor = -1 // invalidate previous vote in new period
// 		rf.role = FOLLOWER
// 	}
// 	reply.Term = args.Term

// 	// TODO: check this logic
// 	if (args.LastLogTerm > rf.logs[len(rf.logs)-1].Term || (args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIdx >= rf.logs[len(rf.logs)-1].Index)) && (rf.votedFor == -1 || rf.votedFor == args.IDcand) {
// 		reply.VoteGranted = true
// 		rf.votedFor = args.IDcand
// 		rf.Heartbeat <- struct{}{} // workaround for the one() test erroring out b/c chosen leader at beginning is not leader at time of check?
// 	} else {
// 		reply.VoteGranted = false
// 	}
// }

// func (rf *RaftServer) sendRequestVote(server string, args *RequestVoteArgs, reply *RequestVoteReply) bool {
// ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

// // dont lock on blocking io
// rf.mu.Lock()
// defer rf.mu.Unlock()

// if ok {
// 	if rf.currentTerm != args.Term || rf.role != CANDIDATE {
// 		return ok
// 	}
// 	if reply.Term > rf.currentTerm {
// 		rf.votedFor = -1
// 		rf.currentTerm = reply.Term
// 		rf.role = FOLLOWER
// 		return ok
// 	}
// 	if reply.VoteGranted {
// 		rf.numVotes++
// 		if rf.numVotes > len(rf.peers)/2 {
// 			nextIdx := rf.logs[len(rf.logs)-1].Index + 1
// 			for i := range rf.peers {
// 				rf.nextIdx[i] = nextIdx
// 			}
// 			rf.role = LEADER
// 			rf.cWinElection <- struct{}{}
// 		}
// 	}
// }
//
// 	return true
// }

// func (rf *RaftServer) sendAppendEntries(server string, arg *AppendEntriesArgs, repl *AppendEntriesResults) {
// rf.mu.Lock()
// // deep copy to prevent that odd non-deterministic data race
// args := &AppendEntriesArgs{
// 	Term:         arg.Term,
// 	LeaderId:     arg.LeaderId,
// 	PrevLogIdx:   arg.PrevLogIdx,
// 	PrevLogTerm:  arg.PrevLogTerm,
// 	LeaderCommit: arg.LeaderCommit,
// 	Entries:      make([]logfile.LogElement, len(arg.Entries)),
// }
// copy(args.Entries, arg.Entries)

// reply := &AppendEntriesResults{
// 	Term:       repl.Term,
// 	Success:    repl.Success,
// 	NextTryIdx: repl.NextTryIdx,
// }
// rf.mu.Unlock()
// // dont block on io
// ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

// rf.mu.Lock()
// defer rf.mu.Unlock()

// baseIdx := rf.logs[0].Index
// if !ok || rf.currentTerm != args.Term || rf.role != LEADER {
// 	return
// }
// if rf.currentTerm < reply.Term {
// 	rf.votedFor = -1
// 	rf.currentTerm = reply.Term
// 	rf.role = FOLLOWER
// 	return
// }
// if reply.Success {
// 	if len(args.Entries) > 0 {
// 		rf.nextIdx[server] = args.Entries[len(args.Entries)-1].Index + 1
// 		rf.matchIdx[server] = rf.nextIdx[server] - 1
// 	}
// } else {
// 	rf.nextIdx[server] = reply.NextTryIdx
// }
// for i := rf.logs[len(rf.logs)-1].Index; i > rf.commitIdx && rf.currentTerm == rf.logs[i-baseIdx].Term; i-- {
// 	count := 1
// 	for j := range rf.peers {
// 		if rf.matchIdx[j] >= i && rf.peers[j] != rf.me {
// 			count++
// 		}
// 	}
// 	if count > len(rf.peers)/2 {
// 		rf.commitIdx = i
// 		go func() {
// 			rf.mu.Lock()
// 			defer rf.mu.Unlock()
// 			for i := rf.appliedLast + 1; i <= rf.commitIdx; i++ {
// 				msg := ApplyMsg{CommandValid: true, Command: rf.logs[i-rf.logs[0].Index].Command, CommandIndex: i}
// 				rf.applyCh <- msg
// 			}
// 			rf.appliedLast = rf.commitIdx
// 		}()
// 		break
// 	}
// }
// }

// `requestVotes` is called when the heartbeat has timed out
// and the raft server turns into a candidate.
// It returns the number of votes received along with error (if any)
func (rf *RaftServer) requestVotes() int {
	var numVotes int = 0

	rf.ReplicaConnMapLock.RLock()
	defer rf.ReplicaConnMapLock.RUnlock()

	// iterate over replica addresses and request
	// vote from each replica
	for _, conn := range rf.ReplicaConnMap {
		electronServiceClient := pb.NewElectionServiceClient(conn)
		response, err := electronServiceClient.Voting(
			context.Background(),
			&pb.VoteRequest{LogfileIndex: uint64(rf.commitIdx)},
		)
		if err != nil {
			log.Printf("[%s] error while requesting vote: %v\n", rf.transport.Addr(), err)
			return 0
		}
		if response.VoteType == pb.VoteResponse_VOTE_GIVEN {
			numVotes += 1
		}
	}
	return numVotes
}

func (rf *RaftServer) sendAppendEntries(server string, grpcArgs *pb.AppendEntriesRequest, reply *pb.AppendEntriesResponse) {
	log.Printf("(sendAppendEntries) %v: Sending append entries to %v", rf.transport.Addr(), server)
	// NOTE! I have removed a deep-copying the AppendEntriesArgs

	// Acquire the connection to the replica
	rf.ReplicaConnMapLock.RLock()
	conn, ok := rf.ReplicaConnMap[server]
	rf.ReplicaConnMapLock.RUnlock()

	if !ok || conn == nil {
		log.Printf("(sendAppendEntries) %v: No connection to server %s", rf.transport.Addr(), server)
		return
	}

	raftClient := pb.NewRaftServiceClient(conn)

	// Execute gRPC call
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	grpcReply, err := raftClient.AppendEntries(ctx, grpcArgs)
	if err != nil {
		log.Printf("(sendAppendEntries) %v: gRPC call to server %s failed: %v", rf.transport.Addr(), server, err)
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
		log.Printf("(sendAppendEntries) %v: Not a leader or term mismatch during RPC to %s", rf.transport.Addr(), server)
		return
	}

	if rf.currentTerm < int(reply.Term) {
		log.Printf("(sendAppendEntries) %v: Other node has later term %d. Becoming follower ", rf.transport.Addr(), reply.Term)
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
				// timeoutHeartbeat := time.Duration(rand.Intn(200)+1100) * time.Millisecond
				rf.Heartbeat = heartbeat.NewHeartbeat(HEARTBEAT_TIMEOUT, func() {
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
			log.Printf("Elections Started...\n")
			rf.mu.Unlock()

			votesWon := rf.requestVotes()

			rf.mu.Lock()
			totalVotes := 1 + votesWon
			totalCandidates := 1 + len(rf.ReplicaConnMap)
			// a candidate wins the election and becomes a leader
			// if it receives more than half of the total votes
			if totalVotes >= totalCandidates/2 {
				// if it wins the election, turn it into a LEADER
				// and start sending heartbeat process
				rf.currentTerm++
				rf.role = LEADER
				rf.leaderAddr = rf.transport.Addr()

				log.Printf("[%v] LEADER in term %d", rf.transport.Addr(), rf.currentTerm)

				// if the replica becomes a LEADER, it does not need to listen
				// for heartbeat from other replicas anymore, so stop the
				// heartbeat timeout process if exists
				rf.Heartbeat.Stop()
			} else {
				// if it loses the election, turn it back in to a follower
				log.Printf("[%s] CANDIDATE: Lost election, reverting to FOLLOWER\n", rf.transport.Addr())
				rf.role = FOLLOWER
			}
			rf.mu.Unlock()

		case LEADER:
			rf.mu.Unlock()

			// the LEADER will send heartbeat to the FOLLOWERS
			go rf.broadcastHeartbeat()

			// limited to 10 heartbeats per second
			time.Sleep(HEARTBEAT_PERIOD)
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
	pb.RegisterBootstrapServiceServer(grpcServer, NewBootstrapServiceServer(rf)) // keep the same
	pb.RegisterRaftServiceServer(grpcServer, NewRaftServiceServer(rf))           // generalize it

	// pb.RegisterHeartbeatServiceServer(grpcServer, NewHeartbeatServiceServer(rf)) // modified
	// pb.RegisterElectionServiceServer(grpcServer, NewElectionServiceServer(rf))
	// pb.RegisterReplicateOperationServiceServer(grpcServer, NewReplicateOpsServiceServer(rf))

	if err = grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve gRPC on port %s: %v", rf.transport.Addr(), err)
	}
	return nil
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
	rf.votedFor = NotVoted
	rf.cWinElection = make(chan struct{}, 1000)

	rf.transport = &GRPCTransport{ListenAddr: me}
	rf.applyCh = applyCh
	rf.logfile = logfile.NewLogfile()
	rf.ReplicaConnMap = make(map[string]*grpc.ClientConn)
	rf.mu.Unlock()

	return rf
}

func (rf *RaftServer) Start() error {
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
