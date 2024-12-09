package main

import (
	"os"
	"time"

	"6.824/log"
	"6.824/raft"
	"6.824/raft/pb"
)

func main() {
	log.SetDebug(true)

	if len(os.Args) < 2 {
		log.DPrintf("Usage: node_client <address> [peer1 peer2 ...]")
		return
	}

	addr := os.Args[1]
	peers := []string{}
	if len(os.Args) > 2 {
		peers = os.Args[2:]
	}

	applyCh := make(chan *pb.LogElement)
	log.DPrintf("Starting Raft server on %s with peers: %v", addr, peers)
	rfServer := raft.MakeRaftServer(addr, applyCh, peers)
	if err := rfServer.StartRaftServer(); err != nil {
		log.DPrintf("Error starting Raft server: %v", err)
		return
	}

	time.Sleep(time.Second * 1000)
}
