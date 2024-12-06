package raft

import (
	"context"
	"fmt"

	"6.824/log"
	"6.824/raft/pb"
)

type HeartbeatServiceServer struct {
	pb.UnimplementedHeartbeatServiceServer
	raftServer *RaftServer
}

func NewHeartbeatServiceServer(s *RaftServer) *HeartbeatServiceServer {
	return &HeartbeatServiceServer{raftServer: s}
}

func (s *HeartbeatServiceServer) Heartbeat(ctx context.Context, request *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// this logic executes when leader sends heartbeat to
	// a follower, the follower will respond with ACK
	log.DPrintf("[%s] received leader heartbeat", s.raftServer.Transport.Addr())

	// set leaderAddr
	s.raftServer.leaderAddr = request.Addr

	// reset heartbeat timer
	if s.raftServer.Heartbeat != nil {
		s.raftServer.Heartbeat.Beat()
		return &pb.HeartbeatResponse{IsAlive: true, Addr: s.raftServer.Transport.Addr()}, nil
	}
	return &pb.HeartbeatResponse{IsAlive: true, Addr: s.raftServer.Transport.Addr()},
		fmt.Errorf("replica not ready")
}
