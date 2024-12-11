package main

import (
	"os"

	"6.824/log"
	"6.824/raft"
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

	log.DPrintf("Starting Raft server on %s with peers: %v", addr, peers)
	rfServer := raft.MakeRaftServer(addr, peers)
	if err := rfServer.StartRaftServer(); err != nil {
		log.DPrintf("Error starting Raft server: %v", err)
		return
	}

	// running indefinitely
	// till process killed
	select {}
}
