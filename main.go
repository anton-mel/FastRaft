package main

import (
	"log"
	"time"
)

func main() {
	s1 := StartKVServer(":3000", []string{})
	s2 := StartKVServer(":4000", []string{":3000"})
	s3 := StartKVServer(":5000", []string{":3000", ":4000"})

	time.Sleep(time.Second * 2) // let the system start properly

	// start sending operations from client
	s1.ApplyOperation("PUT:10,20")

	time.Sleep(time.Second * 1)

	// check if the operation was replicated across clusters
	log.Printf("s1 KV map: %v, s2 KV map: %v, s3 KV map: %v\n", s1.kv, s2.kv, s3.kv)
}
