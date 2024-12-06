package simple_test

import (
	"flag"
	"testing"
	"time"

	"6.824/raft"
)

func TestRaftGeneral(t *testing.T) {
	flag.Parse()

	// This is just an example of the unit test
	s1 := raft.StartKVServer(":1000", []string{})
	s2 := raft.StartKVServer(":1001", []string{":1000"})
	s3 := raft.StartKVServer(":1002", []string{":1001", ":1000"})

	time.Sleep(time.Second * 10) // let the system start properly

	s1.ApplyOperation("SOME_COMMAND_1")
	s2.ApplyOperation("SOME_COMMAND_2")
	s3.ApplyOperation("SOME_COMMAND_3")

	s1.Log_KV()
	s2.Log_KV()
	s3.Log_KV()
}
