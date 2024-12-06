package raft

import (
	"context"

	"6.824/log"
	"6.824/raft/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type BootstrapServiceServer struct {
	pb.UnimplementedBootstrapServiceServer
	raftServer *RaftServer
}

func NewBootstrapServiceServer(s *RaftServer) *BootstrapServiceServer {
	return &BootstrapServiceServer{raftServer: s}
}

func (s *BootstrapServiceServer) AddReplica(ctx context.Context, addrInfo *pb.AddrInfo) (*pb.AddrInfoStatus, error) {
	log.DPrintf("[%s] received (addReplica) request from [%s]", s.raftServer.Transport.Addr(), addrInfo.Addr)

	if ok := s.raftServer.ReplicaConnMap[addrInfo.Addr]; ok != nil {
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
		log.DPrintf("[%s] error while creating gRPC client to [%s]", s.raftServer.Transport.Addr(), addrInfo.Addr)
		return &pb.AddrInfoStatus{IsAdded: false}, err
	}

	s.raftServer.ReplicaConnMapLock.Lock()
	s.raftServer.ReplicaConnMap[addrInfo.Addr] = conn
	s.raftServer.ReplicaConnMapLock.Unlock()

	return &pb.AddrInfoStatus{IsAdded: true}, nil
}