package main

import (
	"context"

	"6.824/pb"
)

type ElectionServiceServer struct {
	pb.UnimplementedElectionServiceServer
	raftServer *RaftServer
}

func NewElectionServiceServer(s *RaftServer) *ElectionServiceServer {
	return &ElectionServiceServer{
		raftServer: s,
	}
}

func (s *ElectionServiceServer) Voting(ctx context.Context, vote *pb.VoteRequest) (*pb.VoteResponse, error) {
	voteType := pb.VoteResponse_VOTE_REFUSED
	// only vote if the requesting raft replica has
	// a longer or same length logfile
	if uint64(s.raftServer.commitIdx) < vote.LogfileIndex && s.raftServer.role != LEADER {
		voteType = pb.VoteResponse_VOTE_GIVEN
	}
	return &pb.VoteResponse{VoteType: voteType}, nil
}
