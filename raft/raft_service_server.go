package raft

import (
	"context"
	"fmt"

	"6.824/log"
	"6.824/raft/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type RaftServiceServer struct {
	pb.UnimplementedRaftServiceServer
	rf *RaftServer
}

// Boostrap Serivce

func (s *RaftServiceServer) AddReplica(ctx context.Context, addrInfo *pb.AddrInfo) (*pb.AddrInfoStatus, error) {
	log.DPrintf("[%s] received (addReplica) request from [%s]", s.rf.Transport.Addr(), addrInfo.Addr)

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

	return &pb.AddrInfoStatus{IsAdded: true}, nil
}

// Heartbeat Service

func (s *RaftServiceServer) Heartbeat(ctx context.Context, request *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// this logic executes when leader sends heartbeat to
	// a follower, the follower will respond with ACK
	log.DPrintf("[%s] received leader heartbeat", s.rf.Transport.Addr())

	// set leaderAddr
	s.rf.leaderAddr = request.Addr

	// reset heartbeat timer
	if s.rf.Heartbeat != nil {
		s.rf.Heartbeat.Beat()
		return &pb.HeartbeatResponse{IsAlive: true, Addr: s.rf.Transport.Addr()}, nil
	}
	return &pb.HeartbeatResponse{IsAlive: true, Addr: s.rf.Transport.Addr()},
		fmt.Errorf("replica not ready")
}

// Ellection Service

func NewRaftServiceServer(rf *RaftServer) *RaftServiceServer {
	return &RaftServiceServer{rf: rf}
}

func (s *RaftServiceServer) AppendEntries(ctx context.Context, args *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	s.rf.mu.Lock()
	defer s.rf.mu.Unlock()

	reply := &pb.AppendEntriesResponse{}

	// Current term check
	if s.rf.currentTerm > int(args.Term) {
		reply.Term = int32(s.rf.currentTerm)
		reply.Success = false
		reply.NextTryIdx = int32(s.rf.logfile.Size() + 1)
		return reply, nil
	}

	// Update term if leader's term is higher
	if s.rf.currentTerm < int(args.Term) {
		s.rf.currentTerm = int(args.Term)
		s.rf.votedFor = ""
		s.rf.role = FOLLOWER
	}

	// Reset heartbeat timer
	s.rf.leaderAddr = args.LeaderAddr
	s.rf.Heartbeat.Beat()

	// Check if log contains entry at PrevLogIdx with matching term
	prevLog, err := s.rf.logfile.GetTransactionWithIndex(int(args.PrevLogIdx))
	if err != nil || (prevLog != nil && prevLog.Term != args.PrevLogTerm) {
		reply.Term = int32(s.rf.currentTerm)
		reply.Success = false
		if prevLog == nil {
			reply.NextTryIdx = int32(s.rf.logfile.Size() + 1)
		} else {
			// Find the conflicting term's first index
			for i := args.PrevLogIdx; i >= 0; i-- {
				logEntry, _ := s.rf.logfile.GetTransactionWithIndex(int(i))
				if logEntry.Term != prevLog.Term {
					reply.NextTryIdx = i + 1
					break
				}
			}
		}
		return reply, nil
	}

	// Append new entries
	if len(args.Entries) > 0 {
		for i, entry := range args.Entries {
			_, err := s.rf.logfile.CommitOperation(int(args.PrevLogIdx)+i+1, s.rf.logfile.Size(), entry)
			if err != nil {
				reply.Success = false
				reply.Term = int32(s.rf.currentTerm)
				return reply, err
			}
		}
	}

	// Update commit index if necessary
	if int(args.LeaderCommit) > s.rf.commitIdx {
		s.rf.commitIdx = min(int(args.LeaderCommit), s.rf.logfile.Size())
		go s.rf.applyLogs()
	}

	reply.Success = true
	reply.Term = int32(s.rf.currentTerm)
	return reply, nil
}

func (rf *RaftServer) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.appliedLast < rf.commitIdx {
		rf.appliedLast++
		entry, err := rf.logfile.GetTransactionWithIndex(rf.appliedLast)
		if err != nil {
			continue // Log index mismatch or other issue
		}
		applyMsg := &pb.LogElement{
			Command: entry.Command,
			Index:   entry.Index,
			Term:    entry.Term,
		}
		rf.applyCh <- applyMsg
	}
}

func (s *RaftServiceServer) RequestVote(ctx context.Context, args *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	s.rf.mu.Lock()
	defer s.rf.mu.Unlock()

	reply := &pb.RequestVoteResponse{}

	if int32(s.rf.currentTerm) > args.Term {
		log.DPrintf("[%v] (RequestVote) Rejecting vote request from %v because my term is greater", s.rf.Transport.Addr(), args.CandidatePort)
		reply.VoteGranted = false
		reply.Term = int32(s.rf.currentTerm)
		return reply, nil
	}

	if int32(s.rf.currentTerm) < args.Term {
		log.DPrintf("[%v] (RequestVote) Updating term to %d", s.rf.Transport.Addr(), args.Term)
		s.rf.currentTerm = int(args.Term)
		s.rf.votedFor = "" // invalidate previous vote in new term
		s.rf.role = FOLLOWER
	}
	reply.Term = int32(s.rf.currentTerm)

	lastTxn, _ := s.rf.logfile.GetFinalTransaction()
	if (args.LastLogTerm > lastTxn.Term || (args.LastLogTerm == lastTxn.Term && args.LastLogIdx >= lastTxn.Index)) &&
		(s.rf.votedFor == "" || s.rf.votedFor == args.CandidatePort) {
		log.DPrintf("[%v] (RequestVote) Granting vote to %v", s.rf.Transport.Addr(), args.CandidatePort)
		reply.VoteGranted = true
		s.rf.votedFor = args.CandidatePort
	} else {
		log.DPrintf("[%v] (RequestVote) Rejecting vote request from %v", s.rf.Transport.Addr(), args.CandidatePort)
		reply.VoteGranted = false
	}

	return reply, nil
}

// Replication Service

func (s *RaftServiceServer) CommitOperation(context context.Context, txn *pb.CommitTransaction) (*pb.CommitOperationResponse, error) {
	log.DPrintf("[%s] received (CommitOperation: %s) from leader", s.rf.Transport.Addr(), txn.Operation)
	logfileFinalIndex, err := s.rf.logfile.CommitOperation(
		int(txn.ExpectedFinalIndex),
		s.rf.commitIdx,
		&pb.LogElement{Index: int32(txn.Index), Command: txn.Operation, Term: int32(txn.Term)},
	)
	if err != nil {
		return nil, err
	}
	return &pb.CommitOperationResponse{LogfileFinalIndex: int64(logfileFinalIndex)}, nil
}

func (s *RaftServiceServer) ApplyOperation(context context.Context, txn *pb.ApplyOperationRequest) (*pb.ApplyOperationResponse, error) {
	log.DPrintf("[%s] received (ApplyOperation) from leader", s.rf.Transport.Addr())
	_, err := s.rf.logfile.ApplyOperation()
	if err != nil {
		return nil, err
	}
	s.rf.commitIdx++
	// s.raftServer.applyCh <- appliedTxn
	return nil, nil
}

func (s *RaftServiceServer) ForwardOperation(context context.Context, in *pb.ForwardOperationRequest) (*pb.ForwardOperationResponse, error) {
	txn, err := s.rf.convertToTransaction(in.Operation)
	if err != nil {
		return nil, err
	}
	if err = s.rf.performTwoPhaseCommit(txn); err != nil {
		return nil, err
	}
	return nil, nil
}
