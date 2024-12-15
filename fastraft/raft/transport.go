package raft

type Transport interface {
	Addr() string                   // returns local address
	Dial(*RaftServer, string) error // calls another replica to add us to their replicaList
}
