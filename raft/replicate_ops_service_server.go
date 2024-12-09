package raft

import (
	"context"

	"6.824/log"
	"6.824/raft/pb"
)

type ReplicateOpsServiceServer struct {
	pb.UnimplementedReplicateOperationServiceServer
	raftServer *RaftServer
}

func NewReplicateOpsServiceServer(raftServer *RaftServer) *ReplicateOpsServiceServer {
	return &ReplicateOpsServiceServer{raftServer: raftServer}
}

func (s *ReplicateOpsServiceServer) CommitOperation(context context.Context, txn *pb.CommitTransaction) (*pb.CommitOperationResponse, error) {
	log.DPrintf("[%s] received (CommitOperation: %s) from leader", s.raftServer.Transport.Addr(), txn.Operation)
	logfileFinalIndex, err := s.raftServer.logfile.CommitOperation(
		int(txn.ExpectedFinalIndex),
		s.raftServer.commitIdx,
		&pb.LogElement{Index: int32(txn.Index), Command: txn.Operation, Term: int32(txn.Term)},
	)
	if err != nil {
		return nil, err
	}
	return &pb.CommitOperationResponse{LogfileFinalIndex: int64(logfileFinalIndex)}, nil
}

func (s *ReplicateOpsServiceServer) ApplyOperation(context context.Context, txn *pb.ApplyOperationRequest) (*pb.ApplyOperationResponse, error) {
	log.DPrintf("[%s] received (ApplyOperation) from leader", s.raftServer.Transport.Addr())
	_, err := s.raftServer.logfile.ApplyOperation()
	if err != nil {
		return nil, err
	}
	s.raftServer.commitIdx++
	// s.raftServer.applyCh <- appliedTxn
	return nil, nil
}

func (s *ReplicateOpsServiceServer) ForwardOperation(context context.Context, in *pb.ForwardOperationRequest) (*pb.ForwardOperationResponse, error) {
	txn, err := s.raftServer.convertToTransaction(in.Operation)
	if err != nil {
		return nil, err
	}
	if err = s.raftServer.performTwoPhaseCommit(txn); err != nil {
		return nil, err
	}
	return nil, nil
}
