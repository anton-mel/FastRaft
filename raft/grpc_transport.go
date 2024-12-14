package raft

import (
	"context"
	"fmt"

	"6.824/log"
	"6.824/raft/pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCTransport struct {
	ListenAddr string
}

func (t *GRPCTransport) Addr() string {
	return t.ListenAddr
}

func (t *GRPCTransport) Dial(s *RaftServer, addr string) error {
	// Establish the gRPC connection with insecure credentials
	log.DPrintf("[%s] connecting to [%s]", t.Addr(), addr)
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.DPrintf("could not connect to raft replica: %v", err)
		return fmt.Errorf("could not connect to raft replica: %v", err)
	}

	bootstrapServiceClient := pb.NewRaftServiceClient(conn)
	response, err := bootstrapServiceClient.AddReplica(
		context.Background(),
		&pb.AddrInfo{Addr: t.Addr()},
	)
	if err != nil {
		log.DPrintf("[%s] error while calling bootstapping service: %v", t.Addr(), err)
		return fmt.Errorf("[%s] error while calling bootstapping service: %v", t.Addr(), err)
	}

	if !response.IsAdded {
		log.DPrintf("[%s] replica [%s] rejected the connection", t.Addr(), addr)
		return fmt.Errorf("error while adding replica [%s] to [%s]: %v", t.Addr(), addr, err)
	}

	log.DPrintf("[%s] successfully connected to [%s]", s.Transport.Addr(), addr)

	// everything is fine, so add the connection to replicaConnMap
	s.ReplicaConnMapLock.Lock()
	s.ReplicaConnMap[addr] = conn
	s.ReplicaConnMapLock.Unlock()

	return nil
}
