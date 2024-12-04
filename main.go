package main

import (
	"log"
	"strings"
	"time"
)

var separator string = strings.Repeat("=", 32)

func main() {
	// Launch out UNIT tests here
	// We can copy paste Vanilla Raft helper functions
	s1 := StartKVServer(":1000", []string{})
	s2 := StartKVServer(":1001", []string{":1000"})
	s3 := StartKVServer(":1002", []string{":1001", ":1000"})

	time.Sleep(time.Second * 10) // let the system start properly

	s1.ApplyOperation("SOME_COMMAND_1")
	s2.ApplyOperation("SOME_COMMAND_2")
	s3.ApplyOperation("SOME_COMMAND_3")

	log.Printf("s1 KV map: %v\n", s1.kv)
	log.Printf("s2 KV map: %v\n", s2.kv)
	log.Printf("s3 KV map: %v\n", s3.kv)
}
