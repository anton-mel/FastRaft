package main

import (
	"context"

	"6.824/pb"
)

type RaftServiceServer struct {
	pb.UnimplementedRaftServiceServer
	rf *RaftServer
}

func NewRaftServiceServer(rf *RaftServer) *RaftServiceServer {
	return &RaftServiceServer{rf: rf}
}

func (s *RaftServiceServer) AppendEntries(ctx context.Context, args *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	s.rf.mu.Lock()
	defer s.rf.mu.Unlock()

	reply := &pb.AppendEntriesResponse{}

	// Current term check
	if s.rf.currentTerm > int(args.Term) {
		reply.Term = int32(s.rf.currentTerm)
		reply.Success = false
		reply.NextTryIdx = int32(s.rf.logfile.Size() + 1)
		return reply, nil
	}

	// Update term if leader's term is higher
	if s.rf.currentTerm < int(args.Term) {
		s.rf.currentTerm = int(args.Term)
		s.rf.votedFor = ""
		s.rf.role = FOLLOWER
	}

	// Reset heartbeat timer
	s.rf.Heartbeat.Beat()

	// Check if log contains entry at PrevLogIdx with matching term
	prevLog, err := s.rf.logfile.GetTransactionWithIndex(int(args.PrevLogIdx))
	if err != nil || (prevLog != nil && prevLog.Term != args.PrevLogTerm) {
		reply.Term = int32(s.rf.currentTerm)
		reply.Success = false
		if prevLog == nil {
			reply.NextTryIdx = int32(s.rf.logfile.Size() + 1)
		} else {
			// Find the conflicting term's first index
			for i := args.PrevLogIdx; i >= 0; i-- {
				logEntry, _ := s.rf.logfile.GetTransactionWithIndex(int(i))
				if logEntry.Term != prevLog.Term {
					reply.NextTryIdx = i + 1
					break
				}
			}
		}
		return reply, nil
	}

	// Append new entries
	if len(args.Entries) > 0 {
		for i, entry := range args.Entries {
			_, err := s.rf.logfile.CommitOperation(int(args.PrevLogIdx)+i+1, s.rf.logfile.Size(), entry)
			if err != nil {
				reply.Success = false
				reply.Term = int32(s.rf.currentTerm)
				return reply, err
			}
		}
	}

	// Update commit index if necessary
	if int(args.LeaderCommit) > s.rf.commitIdx {
		s.rf.commitIdx = min(int(args.LeaderCommit), s.rf.logfile.Size())
		go s.rf.applyLogs()
	}

	reply.Success = true
	reply.Term = int32(s.rf.currentTerm)
	return reply, nil
}

func (rf *RaftServer) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.appliedLast < rf.commitIdx {
		rf.appliedLast++
		entry, err := rf.logfile.GetTransactionWithIndex(rf.appliedLast)
		if err != nil {
			continue // Log index mismatch or other issue
		}
		applyMsg := &pb.LogElement{
			Command: entry.Command,
			Index:   entry.Index,
			Term:    entry.Term,
		}
		rf.applyCh <- applyMsg
	}
}
