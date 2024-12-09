package main

import (
	"context"
	"fmt"
	"time"

	"6.824/log"
	"6.824/raft/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	node1 = ":1000"
	node2 = ":1001"
)

type LoadDriver struct {
	connections []*grpc.ClientConn
	clients     []pb.LoadDriverServiceClient
}

func NewLoadDriver(nodeAddresses []string) (*LoadDriver, error) {
	var connections []*grpc.ClientConn
	var clients []pb.LoadDriverServiceClient

	// Connect to each node in nodeAddresses
	for _, addr := range nodeAddresses {
		conn, err := grpc.NewClient(
			addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
		}
		connections = append(connections, conn)
		clients = append(clients, pb.NewLoadDriverServiceClient(conn))
	}

	return &LoadDriver{connections: connections, clients: clients}, nil
}

func (ld *LoadDriver) Close() {
	for _, conn := range ld.connections {
		conn.Close()
	}
}

func (ld *LoadDriver) ApplyCommand(nodeIndex int, command string) error {
	client := ld.clients[nodeIndex]
	_, err := client.ApplyCommand(context.Background(), &pb.ApplyCommandRequest{
		NodeAddress: "",
		Command:     command,
	})
	return err
}

func (ld *LoadDriver) GetLogs(nodeIndex int) ([]*pb.LogElement, error) {
	client := ld.clients[nodeIndex]
	resp, err := client.GetLogs(context.Background(), &pb.GetLogsRequest{
		NodeAddress: "",
	})
	if err != nil {
		return nil, err
	}
	return resp.Logs, nil
}

func main() {
	log.SetDebug(true)

	// List of nodes to connect to
	nodeAddresses := []string{node1, node2}

	// Initialize LoadDriver with multiple node addresses
	loadDriver, err := NewLoadDriver(nodeAddresses)
	if err != nil {
		log.DPrintf("Failed to initialize LoadDriver: %v", err)
		return
	}
	defer loadDriver.Close()

	// Apply commands to both nodes
	for i := 0; i < 10; i++ {
		command := fmt.Sprintf("COMMAND_%d", i)
		for idx, node := range nodeAddresses {
			log.DPrintf("Applying command to node [%v]", node)
			err := loadDriver.ApplyCommand(idx, command)
			if err != nil {
				log.DPrintf("Failed to apply command to %s: %v", node, err)
			}
		}
		// Slow down if needed
		time.Sleep(5000 * time.Millisecond)
	}

	// Fetch logs from both nodes
	for idx, node := range nodeAddresses {
		logs, err := loadDriver.GetLogs(idx)
		if err != nil {
			log.DPrintf("Failed to get logs from %s: %v", node, err)
			continue
		}
		log.DPrintf("Logs from node %s:\n", node)
		for _, logEntry := range logs {
			log.DPrintf("- Term: %d, Index: %d, Command: %s\n", logEntry.Term, logEntry.Index, logEntry.Command)
		}
	}
}
