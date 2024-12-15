package raft

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"6.824/log"
	"6.824/raft/pb"

	"golang.org/x/exp/rand"
	"google.golang.org/grpc"
)

const (
	FOLLOWER = iota
	CANDIDATE
	LEADER
)

type RaftServer struct {
	state int      // Current replicas' role
	peers []string // now IP:port for of all the peers
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
	cHeartbeat   chan struct{}

	connectionsCount   int
	cBootstrapComplete chan struct{} // avoid split vote when manually launching the nodes
	// this happens because in the for loop startup there is still timeout allowing one to
	// become a leader before voting for itself. Otherwise, eveyone is a CANDIDATE that voted
	// for himself. And since majority cannot be found, this is a deadlock.

	Transport Transport
	applyCh   chan *pb.LogElement
	log       []*pb.LogElement

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
	return rf.state == LEADER
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

// `sendOperationToLeader` is called when an operation reaches a FOLLOWER.
// This function forwards the operation to the LEADER
func sendOperationToLeader(operation string, conn *grpc.ClientConn) error {
	replicateOpsClient := pb.NewRaftServiceClient(conn)
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
	log.DPrintf("[%v] (PerformOperation) IF leader: [%v]", rf.Transport.Addr(), isLeader)

	// only the LEADER is allowed to perform the operation
	// and it then replicates that operation across all the nodes.
	// if the current node is not a LEADER, the operation request
	// will be forwarded to the LEADER, who will then perform the operation
	if isLeader {
		rf.performCommit(command)
		return nil
	}
	log.DPrintf("[%s] (PerformOperation) forwarding operation (%s) to leader [%s]", rf.Transport.Addr(), command, rf.leaderAddr)
	rf.ReplicaConnMapLock.RLock()
	defer rf.ReplicaConnMapLock.RUnlock()

	if rf.leaderAddr == "" {
		log.DPrintf("(PerformOperation) Leader address is not set, cannot forward command <%v>", command)
		return errors.New("leader address is not set")
	}

	// sending operation to the LEADER to perform a TwoPhaseCommit
	return sendOperationToLeader(command, rf.ReplicaConnMap[rf.leaderAddr])
}

// Performs a two phase commit on all the FOLLOWERS
func (rf *RaftServer) performCommit(command string) {
	rf.log = append(rf.log, &pb.LogElement{
		Term:    int32(rf.currentTerm),
		Command: command,
		Index:   rf.log[len(rf.log)-1].Index + 1,
	},
	)
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	grpcReply, err := raftClient.RequestVote(ctx, args)
	if err != nil {
		log.DPrintf("[%v] (sendRequestVote) gRPC call to server %s failed: %v", rf.Transport.Addr(), server, err)
		return false
	}

	// dont lock on blocking io
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Process gRPC response
	if rf.currentTerm != int(args.Term) || rf.state != CANDIDATE {
		log.DPrintf("[%v] (sendRequestVote) is not a candidate or behind arg term", rf.Transport.Addr())
		return true
	}
	if grpcReply.Term > int32(rf.currentTerm) {
		log.DPrintf("[%v] (sendRequestVote) Updating my term to %d", rf.Transport.Addr(), grpcReply.Term)
		rf.votedFor = ""
		rf.currentTerm = int(grpcReply.Term)
		rf.state = FOLLOWER
		return true
	}
	if grpcReply.VoteGranted {
		log.DPrintf("[%v] (sendRequestVote) Received vote from %v", rf.Transport.Addr(), server)
		rf.numVotes++
		if rf.numVotes > len(rf.peers)/2 {
			log.DPrintf("[%v] (sendRequestVote) Won election", rf.Transport.Addr())
			// rf.persist()
			nextIdx := rf.log[len(rf.log)-1].Index + 1
			for _, addr := range rf.peers {
				rf.nextIdx[addr] = int(nextIdx)
			}
			rf.state = LEADER
			rf.leaderAddr = rf.Transport.Addr()
			rf.cWinElection <- struct{}{}
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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

	baseIdx := int(rf.log[0].Index)
	if rf.currentTerm != int(grpcArgs.Term) || rf.state != LEADER {
		// Log if the role/term has changed
		log.DPrintf("[%v] (sendAppendEntries) Not a leader or term mismatch during RPC to %s", rf.Transport.Addr(), server)
		return
	}

	if rf.currentTerm < int(reply.Term) {
		log.DPrintf("[%v] (sendAppendEntries) Other node has later term %d. Becoming follower ", rf.Transport.Addr(), reply.Term)
		// Update to follower if term mismatch
		rf.votedFor = ""
		rf.currentTerm = int(reply.Term)
		rf.state = FOLLOWER
		// rf.persist()
		return
	}
	if reply.Success {
		// Successful log replication
		if len(grpcArgs.Entries) > 0 {
			rf.nextIdx[server] = int(grpcArgs.Entries[len(grpcArgs.Entries)-1].Index) + 1
			rf.matchIdx[server] = rf.nextIdx[server] - 1
		}
	} else {
		// Adjust next index for failed replication
		rf.nextIdx[server] = int(reply.NextTryIdx)
	}

	// Update commit index based on quorum
	for i := int(rf.log[len(rf.log)-1].Index); i > rf.commitIdx && rf.currentTerm == int(rf.log[i-baseIdx].Term); i-- {
		count := 1
		for peer, match := range rf.matchIdx {
			if match >= i && peer != rf.Transport.Addr() {
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

	for i := rf.appliedLast + 1; i <= rf.commitIdx; i++ {
		// Get i-th transaction to Apply the Message to a SM
		// command := rf.log[i-int(rf.log[0].Index)].Command
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
	baseIdx := rf.log[0].Index

	for _, peerAddr := range rf.peers {
		if rf.state == LEADER && peerAddr != rf.Transport.Addr() {
			// Get the next index for this peer
			nextIdx, exists := rf.nextIdx[peerAddr]
			if !exists {
				log.DPrintf("[%v] (broadcastHeartbeat) nextIdx map does not include [%v] index", rf.Transport.Addr(), peerAddr)
				return
			}

			if nextIdx <= int(baseIdx) {
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
				if args.PrevLogIdx >= baseIdx {
					args.PrevLogTerm = rf.log[args.PrevLogIdx-baseIdx].Term
				}

				// Append log entries if there are new entries
				if nextIdx <= int(rf.log[len(rf.log)-1].Index) {
					args.Entries = rf.log[nextIdx-int(baseIdx):]
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
		switch rf.state {
		case FOLLOWER:
			log.DPrintf("[%v] (ticker) FOLLOWER in term %d", rf.Transport.Addr(), rf.currentTerm)
			rf.mu.Unlock()

			select {
			case <-time.After(time.Duration(rand.Intn(5000)+3000) * time.Millisecond):
				rf.mu.Lock()
				rf.state = CANDIDATE
				// rf.persist()
				rf.mu.Unlock()
			case <-rf.cHeartbeat:
				rf.mu.Lock()
				log.DPrintf("[%v] (ticker) Received heartbeat", rf.Transport.Addr())
				rf.mu.Unlock()
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
			case <-time.After(time.Duration(rand.Intn(3000)+3000) * time.Millisecond):
				rf.mu.Lock()
				rf.state = FOLLOWER
				rf.mu.Unlock()
			case <-rf.cHeartbeat:
				rf.mu.Lock()
				rf.state = FOLLOWER
				log.DPrintf("[%v] (ticker) Received heartbeat", rf.Transport.Addr())
				rf.mu.Unlock()
			}

		case LEADER:
			log.DPrintf("[%v] (ticker) LEADER in term %d", rf.Transport.Addr(), rf.currentTerm)
			rf.mu.Unlock()
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
	args := pb.RequestVoteRequest{
		Term:          int32(rf.currentTerm),
		CandidatePort: rf.Transport.Addr(),
		LastLogIdx:    rf.log[len(rf.log)-1].Index,
		LastLogTerm:   rf.log[len(rf.log)-1].Term,
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

func (rf *RaftServer) waitForIncomingConnections() {
	log.DPrintf("[%s] No initial peers. Waiting for incoming connections...", rf.Transport.Addr())
	for {
		rf.mu.Lock()
		if rf.connectionsCount > 0 {
			log.DPrintf("[%s] Received at least one connection. Proceeding...", rf.Transport.Addr())
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond) // Avoid busy waiting
	}
}

// Sends requests to other replicas so that they can add this
// server to their replicaConnMap (establish gRPC connection)
func (rf *RaftServer) bootstrapNetwork() {
	// If there are no peers initially, wait for connections
	time.Sleep(7 * time.Second)
	if len(rf.peers) == 0 {
		rf.waitForIncomingConnections()
	} else {
		// Try connecting to the specified peers
		wg := &sync.WaitGroup{}
		for _, addr := range rf.peers {
			if len(addr) == 0 || addr == rf.Transport.Addr() {
				continue
			}
			wg.Add(1)
			go func(addr string) {
				defer wg.Done()
				log.DPrintf("[%s] Attempting to connect with [%s]", rf.Transport.Addr(), addr)
				if err := rf.Transport.Dial(rf, addr); err == nil {
					rf.mu.Lock()
					rf.connectionsCount++
					rf.mu.Unlock()
				} else {
					log.DPrintf("[%s]: Dial error while connecting to [%s]: %v", rf.Transport.Addr(), addr, err)
				}
			}(addr)
		}
		wg.Wait()
		if rf.connectionsCount == 0 {
			rf.waitForIncomingConnections()
		}
	}

	// Signal that bootstrapping is complete
	log.DPrintf("[%s] Bootstrapping completed with %d connections", rf.Transport.Addr(), rf.connectionsCount)
	close(rf.cBootstrapComplete)
}

func resolveToIP(service string) (string, error) {
	parts := strings.Split(service, ":")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid service format, expected hostname:port")
	}

	hostname := parts[0]
	port := parts[1]

	ips, err := net.LookupHost(hostname)
	if err != nil {
		if len(ips) == 0 {
			return "", fmt.Errorf("failed to resolve hostname %s: %v", hostname, err)
		} else {
			return ips[0], fmt.Errorf("failed to resolve hostname %s: %v", hostname, err)
		}
	}

	return fmt.Sprintf("%s:%s", ips[0], port), nil
}

func MakeRaftServer(me string, peers []string) *RaftServer {
	time.Sleep(time.Second * 4)
	rf := &RaftServer{}
	peerIPs := make([]string, 0)
	peerList := strings.Split(peers[0][6:], ",")
	me = me[3:]
	log.DPrintf("My IP: %s", me)
	for _, peer := range peerList {
		// peerIPs = append(peerIPs, peer)
		log.DPrintf("Peer: %s", peer)
		ip, err := resolveToIP(peer)
		log.DPrintf("IP: %s", ip)
		if err != nil {
			log.DPrintf("Failed to resolve peer %s: %v", peer, err)
			continue
		}
		if ip == me {
			continue
		}
		peerIPs = append(peerIPs, ip)
	}

	rf.mu.Lock()
	rf.state = FOLLOWER
	rf.peers = peerIPs
	// rf.persister = persister

	rf.currentTerm = 0
	rf.appliedLast = 0
	rf.commitIdx = 0

	rf.nextIdx = make(map[string]int)
	rf.matchIdx = make(map[string]int)

	rf.numVotes = 0
	rf.votedFor = ""
	rf.cWinElection = make(chan struct{}, 1000)
	rf.cHeartbeat = make(chan struct{}, 1000)
	rf.cBootstrapComplete = make(chan struct{}, 10)
	rf.connectionsCount = 0

	rf.Transport = &GRPCTransport{ListenAddr: me}
	rf.log = append(rf.log, &pb.LogElement{Term: 0, Command: "", Index: 0})
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
	time.Sleep(time.Second * 3) // wait for server to start

	log.DPrintf("[%s] Raft Server Started 🚀", rf.Transport.Addr())

	// send request to bootstrapped servers to
	// add this to replica to their `replicaConnMap`
	rf.bootstrapNetwork()

	// Wait for bootstrapping to complete
	// before starting ticker process
	<-rf.cBootstrapComplete

	// start elections after
	// all grpc are setted up
	go rf.ticker()

	return nil
}

// SetUp the gRPC servers
func (rf *RaftServer) startGrpcServer() error {
	lis, err := net.Listen("tcp", ":5000") // rf.Transport.Addr()) //  //
	if err != nil {
		return fmt.Errorf("failed to listen on port %s: %v", rf.Transport.Addr(), err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterRaftServiceServer(grpcServer, NewRaftServiceServer(rf))

	if err = grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("failed to serve gRPC on port %s: %v", rf.Transport.Addr(), err)
	}
	return nil
}
