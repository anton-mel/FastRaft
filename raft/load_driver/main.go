package main

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"6.824/log"
	"6.824/raft/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type LoadDriver struct {
	connections []*grpc.ClientConn
	clients     []pb.RaftServiceClient
}

func NewLoadDriver(nodeAddresses []string) (*LoadDriver, error) {
	var connections []*grpc.ClientConn
	var clients []pb.RaftServiceClient

	for _, addr := range nodeAddresses {
		conn, err := grpc.NewClient(
			addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to %s: %w", addr, err)
		}
		connections = append(connections, conn)
		clients = append(clients, pb.NewRaftServiceClient(conn))
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
		Command: command,
	})
	return err
}

func (ld *LoadDriver) GetLogs(nodeIndex int) ([]*pb.LogElement, error) {
	client := ld.clients[nodeIndex]
	resp, err := client.GetLogs(context.Background(), &pb.GetLogsRequest{})
	if err != nil {
		return nil, err
	}
	return resp.Logs, nil
}

func resolveToIP(service string) (string, error) {
	parts := strings.Split(service, ":")
	if len(parts) != 2 {
		return "", fmt.Errorf("invalid service format, expected hostname:port")
	}

	hostname := parts[0]
	port := parts[1]

	ips, err := net.LookupHost(hostname)
	if err != nil {
		if len(ips) == 0 {
			return "", fmt.Errorf("failed to resolve hostname %s: %v", hostname, err)
		} else {
			return ips[0], fmt.Errorf("failed to resolve hostname %s: %v", hostname, err)
		}
	}

	return fmt.Sprintf("%s:%s", ips[0], port), nil
}

func main() {
	log.SetDebug(true)

	if len(os.Args) < 2 {
		log.DPrintf("Usage: load_driver <address> [peer1 peer2 ...]")
		return
	}

	me := os.Args[1][3:]                             // The manager's address
	peersInput := strings.Split(os.Args[2][6:], ",") // The peers' addresses

	log.DPrintf("Starting LoadDriver on %s with peers:", me)
	peers := []string{}
	for _, peer := range peersInput {
		// resolve to IP addr,
		ip, err := resolveToIP(peer)
		if err != nil {
			log.DPrintf("Failed to resolve %s to IP: %v", peer, err)
			continue
		}
		peers = append(peers, ip)
		log.DPrintf("- %s", ip)
	}

	loadDriver, err := NewLoadDriver(peers)
	if err != nil {
		log.DPrintf("Failed to initialize LoadDriver: %v", err)
		return
	}
	// defer loadDriver.Close()

	// Apply commands
	log.DPrintf("Test to check for log correctness")
	for i := 0; i < 50; i++ {
		command := fmt.Sprintf("COMMAND_%d", i)
		// for idx, peer := range peers {
		idx := rand.Intn(len(peers))
		peer := peers[idx]
		log.DPrintf("Applying command [%s] to peer %s", command, peer)
		if err := loadDriver.ApplyCommand(idx, command); err != nil {
			log.DPrintf("Failed to apply command to %s: %v", peer, err)
		} else {
			log.DPrintf("Command [%s] applied to peer %s", command, peer)
		}
		// }
	}

	time.Sleep(3 * time.Second)

	// Fetch logs
	allLogs := make([][]*pb.LogElement, len(peers))
	for idx, peer := range peers {
		logs, err := loadDriver.GetLogs(idx)
		allLogs[idx] = logs
		if err != nil {
			log.DPrintf("Failed to get logs from %s: %v", peer, err)
			continue
		}
		log.DPrintf("Logs from peer %s:", peer)
		for _, logEntry := range logs {
			log.DPrintf("- Term: %d, Index: %d, Command: %s", logEntry.Term, logEntry.Index, logEntry.Command)
		}
	}

	mismatch := 0
	for i := 0; i < len(allLogs[0]); i++ {
		for idx := range peers {
			if allLogs[0][i].Term != allLogs[idx][i].Term || allLogs[0][i].Command != allLogs[idx][i].Command || allLogs[0][i].Index != allLogs[idx][i].Index {
				log.DPrintf("Log mismatch at index %d: %s != %s", i, allLogs[0][i].Command, allLogs[idx][i].Command)
				mismatch++
			}
		}
	}
	log.DPrintf("Total log mismatches: %d", mismatch)

	// logic to compare outputs of pod logs

	// TODO: load test pods here; throughout, response times, errors should be measured

	numThreads := 50
	numCommandsPerThread := 100
	var wg sync.WaitGroup
	var mu sync.Mutex

	totalErrors := 0
	totalCommands := 0
	totalResponseTime := time.Duration(0)

	wg.Add(numThreads)
	for i := 0; i < numThreads; i++ {
		go func(threadID int) {
			defer wg.Done()

			for j := 0; j < numCommandsPerThread; j++ {
				command := fmt.Sprintf("THREAD_%d_COMMAND_%d", threadID, j)
				nodeIndex := rand.Intn(len(peers))
				peer := peers[nodeIndex]

				startTime := time.Now()
				err := loadDriver.ApplyCommand(nodeIndex, command)
				responseTime := time.Since(startTime)

				mu.Lock()
				totalCommands++
				totalResponseTime += responseTime
				if err != nil {
					totalErrors++
					log.DPrintf("Thread %d: Failed to apply command to %s: %v", threadID, peer, err)
				}
				// else {
				// log.DPrintf("Thread %d: Command [%s] applied to peer %s in %v", threadID, command, peer, responseTime)
				// }
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	averageResponseTime := totalResponseTime / time.Duration(totalCommands)
	log.DPrintf("Load Test Complete")
	log.DPrintf("Total Commands Sent: %d", totalCommands)
	log.DPrintf("Total Errors: %d", totalErrors)
	log.DPrintf("Average Response Time: %v", averageResponseTime)

	loadDriver.Close()
}
