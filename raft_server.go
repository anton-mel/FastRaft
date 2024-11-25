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
	"golang.org/x/exp/rand"

	"google.golang.org/grpc"
)

const NotVoted string = "NONE"

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

const (
	HEARTBEAT_PERIOD  = 100 * time.Millisecond
	HEARTBEAT_TIMEOUT = time.Second * 10
)

type RaftServer struct {
	me    string   // Current replicas' port
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

	transport Transport              // gRPC to join replicas' list
	Heartbeat *heartbeat.Heartbeat   // gRPC for leader connection
	applyCh   chan *logfile.ApplyMsg // Ch to apply state machine updates
	logfile   *logfile.Log           // Keep DB simple (look logfile)

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

func (rf *RaftServer) convertToTransaction(operation string) (*logfile.ApplyMsg, error) {
	return &logfile.ApplyMsg{CommandValid: true, Command: operation, CommandIndex: rf.commitIdx + 1}, nil
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
func (rf *RaftServer) performTwoPhaseCommit(txn *logfile.ApplyMsg) error {
	rf.ReplicaConnMapLock.RLock()
	// wg := &sync.WaitGroup{}

	// First phase of the TwoPhaseCommit: Commit operation

	// CommitOperation on self: Use the Apply method from logfile to commit the operation
	_, err := rf.logfile.Apply(rf.commitIdx)
	if err != nil {
		panic(fmt.Errorf("[%s] %v", rf.transport.Addr(), err))
	}

	log.Printf("[%s] performing commit operation on %d followers\n", rf.transport.Addr(), len(rf.ReplicaConnMap))

	// // Replicate commit to all followers
	// for addr, conn := range rf.ReplicaConnMap {
	// 	replicateOpsClient := pb.NewReplicateOperationServiceClient(conn)
	// 	log.Printf("[%s] sending (CommitOperation: %s) to [%s]\n", rf.transport.Addr(), txn.Command, addr)
	// 	response, err := replicateOpsClient.CommitOperation(
	// 		context.Background(),
	// 		&pb.CommitTransaction{
	// 			ExpectedFinalIndex: int64(rf.commitIdx),
	// 			Index:              int64(txn.CommandIndex),
	// 			Operation:          txn.Command,
	// 			Term:               int64(txn.Term),
	// 		},
	// 	)
	// 	if err != nil {
	// 		log.Printf("[%s] received error in (CommitOperation) from [%s]: %v", rf.transport.Addr(), addr, err)
	// 		// if there is both, an error and a response, the FOLLOWER is missing
	// 		// some logs. So the LEADER will replicate all the missing logs in the FOLLOWER
	// 		if response != nil {
	// 			wg.Add(1)
	// 			go rf.replicateMissingLogs(int(response.LogfileFinalIndex), addr, replicateOpsClient, wg)
	// 		} else {
	// 			return err
	// 		}
	// 	}
	// }

	// // wait for all FOLLOWERS to be consistent
	// wg.Wait()

	// log.Printf("[%s] performing (ApplyOperation) on %d followers\n", rf.transport.Addr(), len(rf.ReplicaConnMap))

	// // Second phase of the TwoPhaseCommit: Apply operation

	// // ApplyOperation on self
	// if _, err := rf.logfile.ApplyOperation(); err != nil {
	// 	panic(err)
	// }

	// for _, conn := range rf.ReplicaConnMap {
	// 	replicateOpsClient := pb.NewReplicateOperationServiceClient(conn)
	// 	_, err := replicateOpsClient.ApplyOperation(
	// 		context.Background(),
	// 		&pb.ApplyOperationRequest{},
	// 	)
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	// rf.ReplicaConnMapLock.RUnlock()

	// rf.commitIdx++ // increment the final commitIndex after applying changes

	// rf.applyCh <- txn

	return nil
}

func (rf *RaftServer) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, (rf.role == LEADER)
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

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

// Figure 2 of Raft paper
type AppendEntriesArgs struct {
	Term         int
	LeaderId     string
	PrevLogIdx   int
	PrevLogTerm  int
	Entries      []logfile.LogElement
	LeaderCommit int
}

type AppendEntriesResults struct {
	Term       int
	Success    bool
	NextTryIdx int
}

// func (rf *RaftServer) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesResults) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	lastLogIdx := rf.logs[len(rf.logs)-1].Index
// 	baseIdx := rf.logs[0].Index
// 	if rf.currentTerm > args.Term {
// 		reply.Success = false
// 		reply.NextTryIdx = lastLogIdx + 1
// 		reply.Term = rf.currentTerm
// 		// rf.persist()
// 		return
// 	}
// 	if rf.currentTerm < args.Term {
// 		rf.votedFor = -1
// 		rf.currentTerm = args.Term
// 		rf.role = FOLLOWER
// 	}
// 	reply.Term = args.Term
// 	rf.Heartbeat <- struct{}{}
// 	if args.PrevLogIdx > lastLogIdx {
// 		reply.NextTryIdx = lastLogIdx + 1
// 		// rf.persist()
// 		return
// 	}
// 	if args.PrevLogIdx >= baseIdx && rf.logs[args.PrevLogIdx-baseIdx].Term != args.PrevLogTerm {
// 		term := rf.logs[args.PrevLogIdx-baseIdx].Term
// 		for i := args.PrevLogIdx - 1; i >= baseIdx; i-- {
// 			if rf.logs[i-baseIdx].Term != term {
// 				reply.NextTryIdx = i + 1
// 				// rf.persist()
// 				return
// 			}
// 		}
// 	} else if args.PrevLogIdx > baseIdx-2 {
// 		rf.logs = rf.logs[:args.PrevLogIdx-baseIdx+1]
// 		rf.logs = append(rf.logs, args.Entries...)
// 		reply.NextTryIdx = len(args.Entries) + args.PrevLogIdx
// 		reply.Success = true
// 		if args.LeaderCommit > rf.commitIdx {
// 			rf.commitIdx = min(args.LeaderCommit, rf.logs[len(rf.logs)-1].Index)
// 			go func() {
// 				rf.mu.Lock()
// 				defer rf.mu.Unlock()
// 				for i := rf.appliedLast + 1; i <= rf.commitIdx; i++ {
// 					msg := ApplyMsg{CommandValid: true, Command: rf.logs[i-rf.logs[0].Index].Command, CommandIndex: i}
// 					rf.applyCh <- msg
// 				}
// 				rf.appliedLast = rf.commitIdx
// 			}()
// 		}
// 	}
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

func (rf *RaftServer) sendHeartbeat() int {
	aliveCount := 0
	rf.ReplicaConnMapLock.RLock()
	for addr, conn := range rf.ReplicaConnMap {
		heartbeatClient := pb.NewHeartbeatServiceClient(conn)
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*1))
		defer cancel()
		response, err := heartbeatClient.Heartbeat(
			ctx,
			&pb.HeartbeatRequest{
				IsAlive: true,
				Addr:    rf.transport.Addr(),
			})
		if err != nil {
			log.Printf("[%s] error while sending heartbeat to [%s]: %v\n", rf.transport.Addr(), addr, err)
		}
		if response != nil && response.IsAlive {
			aliveCount++
		}
	}
	rf.ReplicaConnMapLock.RUnlock()
	return aliveCount
}

// `broadcastHeartbeat` is called by the leader to
// send a heartbeat to followers every second
func (rf *RaftServer) broadcastHeartbeat() {
	// start the process of sending heartbeat for a leader
	for {
		log.Printf("[%s] sending heartbeat to %d followers\n", rf.transport.Addr(), len(rf.ReplicaConnMap))
		aliveReplicas := rf.sendHeartbeat()
		if aliveReplicas < (len(rf.ReplicaConnMap)-1)/2 {
			panic("more than half of the replicas are down")
		}
		// limited to 10 heartbeats per second
		time.Sleep(HEARTBEAT_PERIOD)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *RaftServer) ticker() {
	for {
		timeoutDuration := time.Duration(rand.Intn(200)+1100) * time.Millisecond

		rf.mu.Lock()
		switch rf.role {
		case FOLLOWER:
			// Start heartbeat timeout only if not already set
			if rf.Heartbeat == nil {
				// Initialize a new heartbeat with timeout
				rf.Heartbeat = heartbeat.NewHeartbeat(timeoutDuration, func() {
					log.Printf("[%s] FOLLOWER timeout expired\n", rf.transport.Addr())
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.role = CANDIDATE
				})
			} else if rf.Heartbeat.Expired() {
				// Reset the heartbeat timer if it has expired
				log.Printf("[%s] Restarting FOLLOWER timeout\n", rf.transport.Addr())
				rf.Heartbeat.Beat()
			}

		case CANDIDATE:
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.numVotes = 1

			// Log election timeout
			log.Printf("Starting election\n")

			// Send request vote to other peers
			votesWon := rf.requestVotes()
			totalVotes := 1 + votesWon
			totalCandidates := 1 + len(rf.ReplicaConnMap)
			// a candidate wins the election and becomes a leader
			// if it receives more than half of the total votes
			if totalVotes >= totalCandidates/2 {
				// if it wins the election, turn it into a LEADER
				// and start sending heartbeat process
				log.Printf("[%s] is the leader", rf.transport.Addr())
				rf.role = LEADER
				rf.currentTerm++
				rf.leaderAddr = rf.transport.Addr()

				// if the replica becomes a LEADER, it does not need to listen
				// for heartbeat from other replicas anymore, so stop the
				// heartbeat timeout process
				rf.Heartbeat.Stop()
			} else {
				// if it loses the election, turn it back in to a follower
				rf.role = FOLLOWER
			}

		case LEADER:
			// the LEADER will send heartbeat to the FOLLOWERS
			go rf.broadcastHeartbeat()
		}

		rf.mu.Unlock()
		time.Sleep(timeoutDuration)
	}
}

// Additional: sends requests to other replicas so that they can
// add this server to their replicaConnMap (establish gRPC connection)
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

// Additional: SetUp the gRPC servers
func (rf *RaftServer) startGrpcServer() error {
	lis, err := net.Listen("tcp", rf.transport.Addr())
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %v", rf.transport.Addr(), err)
	}

	// register services with the gRPC server
	grpcServer := grpc.NewServer()

	// I will keep it as a separate services, since it is actually a clean approach
	pb.RegisterBootstrapServiceServer(grpcServer, NewBootstrapServiceServer(rf))
	pb.RegisterHeartbeatServiceServer(grpcServer, NewHeartbeatServiceServer(rf))
	// pb.RegisterElectionServiceServer(grpcServer, NewElectionServiceServer(s))
	// pb.RegisterReplicateOperationServiceServer(grpcServer, NewReplicateOpsServiceServer(s))
	if err = grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve gRPC on port %s: %v", rf.transport.Addr(), err)
	}
	return nil
}

func MakeRaftServer(me string, applyCh chan *logfile.ApplyMsg, peers []string) *RaftServer {
	rf := &RaftServer{}

	rf.mu.Lock()
	rf.me = me
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
	rf.logfile = logfile.NewLog()
	rf.ReplicaConnMap = make(map[string]*grpc.ClientConn)
	rf.mu.Unlock()

	return rf
}

// We cannot do this before grpc is setted up (keep it for reference)
// func (rf *RaftServer) Start(command interface{}) (int, int, bool) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	index := -1
// 	term := -1
// 	isLeader := rf.role == LEADER

// 	if isLeader {
// 		term = rf.currentTerm
// 		index = rf.logs[len(rf.logs)-1].Index + 1
// 		rf.logs = append(rf.logs, logfile.LogElement{Term: term, Command: command, Index: index})
// 	}

// 	return index, term, isLeader
// }

func (rf *RaftServer) Start() error {
	go rf.startGrpcServer()
	time.Sleep(time.Second * 3) // wait for server to start

	log.Printf("[%s] raft server started\n", rf.transport.Addr())

	// send request to bootstrapped servers to
	// add this to replica to their `replicaConnMap`
	rf.bootstrapNetwork()

	// start elections after
	// all grpc are setted up
	go rf.ticker()

	return nil
}
