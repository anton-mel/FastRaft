package raft

import (
	"context"

	"6.824/log"
	"6.824/raft/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftServiceServer struct {
	pb.UnimplementedRaftServiceServer
	rf *RaftServer
}

func NewRaftServiceServer(rf *RaftServer) *RaftServiceServer {
	return &RaftServiceServer{rf: rf}
}

// Bootstrap Service

func (s *RaftServiceServer) AddReplica(ctx context.Context, addrInfo *pb.AddrInfo) (*pb.AddrInfoStatus, error) {
	log.DPrintf("[%s] received (addReplica) request from [%s]", s.rf.Transport.Addr(), addrInfo.Addr)
	if addrInfo.Addr == "0.0.0.0:5000" {
		// [Deployement Testing] comment for local testing
		return &pb.AddrInfoStatus{IsAdded: false}, nil
	}

	if ok := s.rf.ReplicaConnMap[addrInfo.Addr]; ok != nil {
		// raft server already present
		log.DPrintf("raft server [%s] present in replicaConnMap", addrInfo.Addr)
		return &pb.AddrInfoStatus{IsAdded: true}, nil
	}

	// raft server not present in replicaConnMap,
	// create a new connection and add it
	conn, err := grpc.NewClient(
		addrInfo.Addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.DPrintf("[%s] error while creating gRPC client to [%s]", s.rf.Transport.Addr(), addrInfo.Addr)
		return &pb.AddrInfoStatus{IsAdded: false}, err
	}

	s.rf.ReplicaConnMapLock.Lock()
	s.rf.ReplicaConnMap[addrInfo.Addr] = conn
	s.rf.ReplicaConnMapLock.Unlock()

	// let the other replica know
	// when to start the election
	s.rf.connectionsCount++

	return &pb.AddrInfoStatus{IsAdded: true}, nil
}

// Load (Testing) Service

func (s *RaftServiceServer) ApplyCommand(ctx context.Context, req *pb.ApplyCommandRequest) (*pb.ApplyCommandResponse, error) {
	s.rf.PerformOperation(req.Command)
	return &pb.ApplyCommandResponse{Success: true}, nil
}

func (s *RaftServiceServer) GetLogs(ctx context.Context, req *pb.GetLogsRequest) (*pb.GetLogsResponse, error) {
	return &pb.GetLogsResponse{Logs: s.rf.log}, nil
}

// Ellection Service

func (s *RaftServiceServer) AppendEntries(ctx context.Context, args *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	s.rf.mu.Lock()
	defer s.rf.mu.Unlock()

	reply := &pb.AppendEntriesResponse{}
	lastLogIdx := s.rf.log[len(s.rf.log)-1].Index
	baseIdx := s.rf.log[0].Index
	// Current term check
	if s.rf.currentTerm > int(args.Term) {
		reply.Term = int32(s.rf.currentTerm)
		reply.NextTryIdx = lastLogIdx
		reply.Success = false
		return reply, nil
	}
	// Update term if leader's term is higher
	if s.rf.currentTerm < int(args.Term) {
		log.DPrintf("[%v] (AppendEntries) Updating term to %d", s.rf.Transport.Addr(), args.Term)
		s.rf.votedFor = ""
		s.rf.currentTerm = int(args.Term)
		s.rf.state = FOLLOWER
	}
	reply.Term = int32(s.rf.currentTerm)
	s.rf.leaderAddr = args.LeaderAddr
	s.rf.cHeartbeat <- struct{}{}
	if args.PrevLogIdx > lastLogIdx {
		log.DPrintf("(AppendEntries) %v: Rejecting append entries from [%v] because args.PrevLogIdx > lastLogIdx", s.rf.Transport.Addr(), args.LeaderAddr)
		reply.NextTryIdx = lastLogIdx + 1
		return reply, nil
	}
	if args.PrevLogIdx >= baseIdx && s.rf.log[args.PrevLogIdx-baseIdx].Term != args.PrevLogTerm {
		term := s.rf.log[args.PrevLogIdx-baseIdx].Term
		for i := args.PrevLogIdx - 1; i >= baseIdx; i-- {
			if s.rf.log[i-baseIdx].Term != term {
				reply.NextTryIdx = i + 1
				return reply, nil
			}
		}
	} else if args.PrevLogIdx > baseIdx-2 {
		// [Fast Raft] If an existing entry conflicts
		// with a new one, overwrite the existing entry
		s.rf.log = s.rf.log[:args.PrevLogIdx-baseIdx+1]
		s.rf.log = append(s.rf.log, args.Entries...)

		reply.NextTryIdx = int32(len(args.Entries)) + args.PrevLogIdx
		reply.Success = true

		// Update commit index if needed
		if int(args.LeaderCommit) > s.rf.commitIdx {
			s.rf.commitIdx = int(min(args.LeaderCommit, s.rf.log[len(s.rf.log)-1].Index))
			log.DPrintf("[%v] (AppendEntries) Committing logs up to %d", s.rf.Transport.Addr(), s.rf.commitIdx)
			go func() {
				s.rf.mu.Lock()
				defer s.rf.mu.Unlock()
				for i := s.rf.appliedLast + 1; i <= s.rf.commitIdx; i++ {
					s.rf.insertEntry(s.rf.log[i-int(s.rf.log[0].Index)].Command, "leader")
					// msg := ApplyMsg{CommandValid: true, Command: s.rf.log[i-s.rf.log[0].Index].Command, CommandIndex: i}
					// s.rf.cApplyMsg <- msg
				}
				s.rf.appliedLast = s.rf.commitIdx
			}()
		}
	}

	reply.Success = true
	reply.Term = int32(s.rf.currentTerm)
	return reply, nil
}

func (s *RaftServiceServer) RequestVote(ctx context.Context, args *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	s.rf.mu.Lock()
	defer s.rf.mu.Unlock()

	reply := &pb.RequestVoteResponse{
		Term:                int32(s.rf.currentTerm),
		VoteGranted:         false,
		SelfApprovedEntries: []*pb.LogElement{},
	}

	if int32(s.rf.currentTerm) > args.Term {
		log.DPrintf("[%v] (RequestVote) Rejecting vote request from [%v] because my term is greater", s.rf.Transport.Addr(), args.CandidatePort)
		return reply, nil
	}

	// if int32(s.rf.currentTerm) < args.Term {
	// 	log.DPrintf("[%v] (RequestVote) Updating term to %d", s.rf.Transport.Addr(), args.Term)
	// 	s.rf.currentTerm = int(args.Term)
	// 	s.rf.votedFor = "" // invalidate previous vote in new term
	// 	s.rf.state = FOLLOWER
	// }

	// [Fast Raft] Collect all self-approved entries from the log
	selfApprovedEntries := []*pb.LogElement{}
	for _, entry := range s.rf.log {
		if entry.InsertedBy == "self" {
			selfApprovedEntries = append(selfApprovedEntries, entry)
		}
	}

	lastTxn := s.rf.log[len(s.rf.log)-1]

	// if args.LastLogTerm > lastTxn.Term {
	// 	log.DPrintf("[%v] (RequestVote) Granting vote to [%v] (candidate's log term is higher)", s.rf.Transport.Addr(), args.CandidatePort)
	// 	reply.VoteGranted = true
	// 	s.rf.votedFor = args.CandidatePort
	// 	s.rf.cHeartbeat <- struct{}{}
	// } else if args.LastLogTerm == lastTxn.Term && args.LastLogIdx >= lastTxn.Index {

	if s.rf.votedFor == "" || s.rf.votedFor == args.CandidatePort {
		// [Fast Raft] Look (When receiving a RequestVote message from a candidate) Table
		if (int(lastTxn.Index) >= s.rf.lastLeaderIdx && lastTxn.Term >= s.rf.log[s.rf.lastLeaderIdx].Term) || lastTxn.Term > s.rf.log[s.rf.lastLeaderIdx].Term {
			log.DPrintf("[%v] (RequestVote) Granting vote to [%v] (candidate's log is equally up-to-date)", s.rf.Transport.Addr(), args.CandidatePort)
			reply.VoteGranted = true
			s.rf.votedFor = args.CandidatePort
			s.rf.cHeartbeat <- struct{}{}
			reply.SelfApprovedEntries = selfApprovedEntries
		} else {
			log.DPrintf("[%v] (RequestVote) Rejecting vote request from [%v] (candidate's log is not up-to-date)", s.rf.Transport.Addr(), args.CandidatePort)
			reply.VoteGranted = false
		}
	} else {
		log.DPrintf("[%v] (RequestVote) Rejecting vote request from [%v] since already voted for [%v]", s.rf.Transport.Addr(), args.CandidatePort, s.rf.votedFor)
		reply.VoteGranted = false
	}

	return reply, nil
}

// Replication Service

func (s *RaftServiceServer) CommitOperation(context context.Context, txn *pb.CommitTransaction) (*pb.CommitOperationResponse, error) {
	log.DPrintf("[%s] received (CommitOperation: %s) from leader", s.rf.Transport.Addr(), txn.Operation)

	s.rf.mu.Lock()
	defer s.rf.mu.Unlock()

	if int(txn.ExpectedFinalIndex) != len(s.rf.log) {
		return &pb.CommitOperationResponse{LogfileFinalIndex: int64(len(s.rf.log) - 1)}, nil
	}

	newLogEntry := &pb.LogElement{
		Index:      int32(txn.Index),
		Command:    txn.Operation,
		Term:       int32(txn.Term),
		InsertedBy: "leader",
	}
	s.rf.log = append(s.rf.log, newLogEntry)

	return &pb.CommitOperationResponse{LogfileFinalIndex: int64(len(s.rf.log) - 1)}, nil
}

func (s *RaftServiceServer) ApplyOperation(ctx context.Context, txn *pb.ApplyOperationRequest) (*pb.ApplyOperationResponse, error) {
	log.DPrintf("[%s] received (ApplyOperation) from leader", s.rf.Transport.Addr())

	s.rf.mu.Lock()
	defer s.rf.mu.Unlock()

	if s.rf.commitIdx < len(s.rf.log)-1 {
		s.rf.commitIdx++
		// appliedEntry := s.rf.log[s.rf.commitIdx]
		// s.rf.applyCh <- appliedEntry
	}

	return &pb.ApplyOperationResponse{}, nil
}

func (s *RaftServiceServer) ForwardOperation(ctx context.Context, in *pb.ForwardOperationRequest) (*pb.ForwardOperationResponse, error) {
	s.rf.insertEntry(in.Operation, "self")
	return &pb.ForwardOperationResponse{}, nil
}
