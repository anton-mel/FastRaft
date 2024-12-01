package main

import (
	"context"
	"log"

	"6.824/logfile"
	"6.824/pb"
)

type ReplicateOpsServiceServer struct {
	pb.UnimplementedReplicateOperationServiceServer
	raftServer *RaftServer
}

func NewReplicateOpsServiceServer(raftServer *RaftServer) *ReplicateOpsServiceServer {
	return &ReplicateOpsServiceServer{raftServer: raftServer}
}

func (s *ReplicateOpsServiceServer) CommitOperation(context context.Context, txn *pb.CommitTransaction) (*pb.CommitOperationResponse, error) {
	log.Printf("[%s] received (CommitOperation: %s) from leader\n", s.raftServer.transport.Addr(), txn.Operation)
	logfileFinalIndex, err := s.raftServer.logfile.CommitOperation(
		int(txn.ExpectedFinalIndex),
		s.raftServer.commitIdx,
		&logfile.LogElement{Index: int(txn.Index), Command: txn.Operation, Term: int(txn.Term)},
	)
	if err != nil {
		return nil, err
	}
	return &pb.CommitOperationResponse{LogfileFinalIndex: int64(logfileFinalIndex)}, nil
}

func (s *ReplicateOpsServiceServer) ApplyOperation(context context.Context, txn *pb.ApplyOperationRequest) (*pb.ApplyOperationResponse, error) {
	log.Printf("[%s] received (ApplyOperation) from leader\n", s.raftServer.transport.Addr())
	appliedTxn, err := s.raftServer.logfile.ApplyOperation()
	if err != nil {
		return nil, err
	}
	s.raftServer.commitIdx++
	s.raftServer.applyCh <- appliedTxn
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
