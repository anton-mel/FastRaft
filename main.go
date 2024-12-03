package main

import (
	"log"
	"strings"
	"time"
)

var separator string = strings.Repeat("=", 32)

func main() {
	s1 := StartKVServer(":1000", []string{})
	s2 := StartKVServer(":1001", []string{":1000"})
	s3 := StartKVServer(":1002", []string{":1001", ":1000"})

	time.Sleep(time.Second * 3) // let the system start properly

	// start sending operations from client
	// s1.ApplyOperation("SOME_COMMAND_STR1")
	// s2.ApplyOperation("SOME_COMMAND_STR2")
	// s3.ApplyOperation("SOME_COMMAND_STR3")

	// check if the operation was replicated across clusters
	log.Printf("s1 KV map: %v\n", s1.kv)
	log.Printf("s2 KV map: %v\n", s2.kv)
	log.Printf("s3 KV map: %v\n", s3.kv)
}
