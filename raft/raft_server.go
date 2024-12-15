package raft

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
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

// [Fast Raft] requires a larger
// quorum for the fast path.
const FAST_QUORUM_FACTOR = 0.75

type RaftServer struct {
	state int      // Current replicas' role
	peers []string // now IP:port for of all the peers

	leaderAddr  string
	currentTerm int
	appliedLast int
	commitIdx   int
	// index of highest log
	// entry known to be commited

	lastLeaderIdx int // [Fast Raft]

	nextIdx  map[string]int // only for leaders
	matchIdx map[string]int // only for leaders

	// fastMatchIdx, separate from matchIdx for determining
	// if an entry can be committed on the fast track or classic track.
	fastMatchIdx map[string]int // only for leaders [Fast Raft]
	// A leader in classic Raft would immediately
	// append entries proposed to it. However, Fast Raft needs a
	// method by which to keep track of the votes of followers for
	// a log index. The leader makes its decision on what entry to
	// insert or commit based on the contents of possibleEntries.
	possibleEntries map[int]map[string]int // only for leaders [Fast Raft]

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

// [Fast Raft] return a sufficient for leader
// quorum size to commit entry e given M sites.
func (rf *RaftServer) fastQuorumSize() int {
	return int(math.Ceil(float64(len(rf.peers)) * FAST_QUORUM_FACTOR))
}

// [Fast Raft] isFastTrackPossible checks if
// fast-tracking is possible for the given entry.
func (rf *RaftServer) isFastTrackPossible(entry *pb.LogElement) bool {
	// Fast track if the entry has 3M/4 support from followers
	return rf.fastMatchIdx[entry.InsertedBy] >= rf.fastQuorumSize()
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

// `sendOperationToSite` is called when an operation reaches a FOLLOWER.
// This function forwards the operation to other PEERS FOLLOWERS.
func sendOperationToSite(operation string, conn *grpc.ClientConn) error {
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
	log.DPrintf("[%v] (PerformOperation) Is Leader: [%v]", rf.Transport.Addr(), isLeader)

	if isLeader {
		// Leader directly appends the entry and starts replication
		// This is same as Fast-Raft, replicate normally.
		rf.insertEntry(command, "leader")
		return nil
	} else {
		// [Fast Raft] Not leader? Try Fast Path...
		rf.insertEntry(command, "self")

		// In Fast Raft, non-leader nodes can receive and process
		// proposals directly from clients. This is different from traditional
		// Raft, where all proposals go through the leader.
		log.DPrintf("[%s] (PerformOperation) forwarding operation (%s) to all peers", rf.Transport.Addr(), command)

		rf.ReplicaConnMapLock.RLock()
		defer rf.ReplicaConnMapLock.RUnlock()

		if rf.leaderAddr == "" {
			log.DPrintf("(PerformOperation) Leader address is not set, cannot forward command <%v>", command)
			return errors.New("leader address is not set")
		}

		// Send then entry to to all sites
		for peerAddr, conn := range rf.ReplicaConnMap {
			// [Check Later] Skip forwarding to the leader
			// Should be done from the follower, right?
			if peerAddr == rf.leaderAddr {
				continue
			}
			// Replication is by-default marked as self-approved
			err := sendOperationToSite(command, conn)
			if err != nil {
				// Log the error but continue to other peers
				log.DPrintf("(PerformOperation) Failed to send command <%v> to peer [%s]: %v", command, peerAddr, err)
			} else {
				log.DPrintf("(PerformOperation) Successfully sent command <%v> to peer [%s]", command, peerAddr)
			}
		}

		return nil
	}
}

// Performs a two phase commit on all the FOLLOWERS
func (rf *RaftServer) insertEntry(command string, insertedBy string) {
	rf.log = append(rf.log, &pb.LogElement{
		Term:       int32(rf.currentTerm),
		Command:    command,
		Index:      rf.log[len(rf.log)-1].Index + 1,
		InsertedBy: insertedBy, // [Fast Raft] either self or leader
	})
}

// [Fast Raft] recover fn recovers self-approved entries
// and commits them if they were already committed by
// a previous leader or can be fast-tracked.
func (rf *RaftServer) recover() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// If the leader inserts an entry but does not commit it,
	// we revert to the classic track, which is identical to classic
	// Raft. The leader sends AppendEntries messages to have
	// a classic quorum insert the entry that had the most votes.
	// Entries inserted on the classic track are marked as leaderapproved.
	// If a follower receives an entry from the leader that
	// it already inserted, it will update it to be leader-approved.
	// Note, the classic track only occurs after attempting the fast
	// track, and thus, in this situation, we suffer the penalty of an
	// extra message round compared to classic Raft.
	// Read Section III (B) for better explanation.

	// Collect self-approved
	// entries (entries inserted by followers)
	selfApprovedEntries := make([]*pb.LogElement, 0)
	for _, entry := range rf.log {
		if entry.InsertedBy != "leader" {
			selfApprovedEntries = append(selfApprovedEntries, entry)
		}
	}

	// Process each self-approved entry
	for _, entry := range selfApprovedEntries {
		// Check if any previous leader committed this entry
		if rf.isCommittedByPreviousLeader(entry) {
			// [Step 3] If the entry is already committed
			// by a previous leader, commit it immediately
			// NOTE: we do not need this in a simple log
			// replication; we would need to create
			// additionally applyCommittedEntry.
			log.DPrintf("[%v] (commitEntry) Committing entry with index %d", rf.Transport.Addr(), entry.Index)
			rf.commitIdx = int(entry.Index)
		} else {
			// [Step 4] If the entry is not committed
			// by a previous leader, fast track it
			if rf.isFastTrackPossible(entry) {
				rf.commitIdx = int(entry.Index)
			} else {
				// [Step 5] If fast tracking is not possible,
				// leave it for classic Raft handling
				entry.InsertedBy = "leader"
				rf.commitIdx = int(entry.Index)
			}
		}
	}
}

// [Fast Raft] isCommittedByPreviousLeader checks if
// the entry has been committed by any previous leader.
func (rf *RaftServer) isCommittedByPreviousLeader(entry *pb.LogElement) bool {
	// Check if this entry has been committed by any leader
	for _, matchIdx := range rf.fastMatchIdx {
		if matchIdx >= int(entry.Index) {
			return true
		}
	}
	return false
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
			// [Fast Raft] Leader election follows the same flow as in
			// classic Raft with some alterations. Self-approved entries
			// cannot be considered in this check, as proposers can send
			// an arbitrarily large number of proposals to a follower
			// that ultimately may not have been agreed upon.
			lastLog := rf.log[len(rf.log)-1]
			if lastLog.InsertedBy == "leader" {
				log.DPrintf("[%v] (sendRequestVote) Won election with leader-approved logs", rf.Transport.Addr())
				// rf.persist()
				nextIdx := lastLog.Index + 1
				for _, addr := range rf.peers {
					rf.nextIdx[addr] = int(nextIdx)
				}
				rf.state = LEADER
				rf.leaderAddr = rf.Transport.Addr()

				// Once the most up-to-date candidate is elected, Fast Raft
				// runs a recovery algorithm. Self-approved entries were not
				// considered in the election, and need to be evaluated to
				// ensure safety. All followers resend their self-approved
				// entries to the newly elected leader. If a leader from a
				// previous term committed any of these entries, then a there
				// will be a fast quorum that has inserted the entry, and the
				// new leader will make the same decision as previous leaders
				// and commit the entry.

				rf.mu.Unlock()
				rf.recover() // [Fast Raft] Handle self-approved entries
				rf.mu.Lock()

				rf.cWinElection <- struct{}{}
			} else {
				log.DPrintf("[%v] (sendRequestVote) Logs not leader-approved, staying as candidate", rf.Transport.Addr())
			}
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
			case <-time.After(time.Duration(rand.Intn(5000)+1100) * time.Millisecond):
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
			case <-time.After(time.Duration(rand.Intn(5000)+600) * time.Millisecond):
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

// Sends requests to other replicas so that they can add this
// server to their replicaConnMap (establish gRPC connection)
func (rf *RaftServer) bootstrapNetwork() {
	// If there are no peers initially, wait for connections
	if len(rf.peers) == 0 {
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
	} else {
		// Try connecting to the specified peers
		wg := &sync.WaitGroup{}
		for _, addr := range rf.peers {
			if len(addr) == 0 {
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
	}

	// Signal that bootstrapping is complete
	log.DPrintf("[%s] Bootstrapping completed with %d connections", rf.Transport.Addr(), rf.connectionsCount)
	close(rf.cBootstrapComplete)
}

func MakeRaftServer(me string, peers []string) *RaftServer {
	rf := &RaftServer{}

	rf.mu.Lock()
	rf.state = FOLLOWER
	rf.peers = peers
	// rf.persister = persister

	rf.currentTerm = 0
	rf.appliedLast = 0
	rf.commitIdx = 0

	rf.lastLeaderIdx = 0

	rf.nextIdx = make(map[string]int)
	rf.matchIdx = make(map[string]int)
	rf.fastMatchIdx = make(map[string]int)
	rf.possibleEntries = make(map[int]map[string]int)

	rf.numVotes = 0
	rf.votedFor = ""
	rf.cWinElection = make(chan struct{}, 1000)
	rf.cHeartbeat = make(chan struct{}, 1000)
	rf.cBootstrapComplete = make(chan struct{}, 10)
	rf.connectionsCount = 0

	rf.Transport = &GRPCTransport{ListenAddr: me}
	rf.log = append(rf.log, &pb.LogElement{Term: 0, Command: "", Index: 0, InsertedBy: "leader"})
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
	lis, err := net.Listen("tcp", rf.Transport.Addr())
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
