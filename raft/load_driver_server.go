package raft

import (
	"context"

	"6.824/raft/pb"
)

type LoadDriverServiceServer struct {
	pb.UnimplementedLoadDriverServiceServer
	rf *RaftServer
}

func NewLoadDriverServiceServer(rf *RaftServer) *LoadDriverServiceServer {
	return &LoadDriverServiceServer{rf: rf}
}

func (s *LoadDriverServiceServer) ApplyCommand(ctx context.Context, req *pb.ApplyCommandRequest) (*pb.ApplyCommandResponse, error) {
	s.rf.PerformOperation(req.Command)
	return &pb.ApplyCommandResponse{Success: true}, nil
}

func (s *LoadDriverServiceServer) GetLogs(ctx context.Context, req *pb.GetLogsRequest) (*pb.GetLogsResponse, error) {
	logs := s.rf.logfile.GetAllLogs()
	return &pb.GetLogsResponse{Logs: logs}, nil
}
