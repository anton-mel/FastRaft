// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.28.3
// source: raft/proto/raft.proto

package pb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	RaftService_AddReplica_FullMethodName       = "/proto.RaftService/addReplica"
	RaftService_AppendEntries_FullMethodName    = "/proto.RaftService/AppendEntries"
	RaftService_RequestVote_FullMethodName      = "/proto.RaftService/RequestVote"
	RaftService_ApplyCommand_FullMethodName     = "/proto.RaftService/ApplyCommand"
	RaftService_GetLogs_FullMethodName          = "/proto.RaftService/GetLogs"
	RaftService_CommitOperation_FullMethodName  = "/proto.RaftService/CommitOperation"
	RaftService_ApplyOperation_FullMethodName   = "/proto.RaftService/ApplyOperation"
	RaftService_ForwardOperation_FullMethodName = "/proto.RaftService/ForwardOperation"
)

// RaftServiceClient is the client API for RaftService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
//
// Raft service definition
type RaftServiceClient interface {
	AddReplica(ctx context.Context, in *AddrInfo, opts ...grpc.CallOption) (*AddrInfoStatus, error)
	AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error)
	RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error)
	ApplyCommand(ctx context.Context, in *ApplyCommandRequest, opts ...grpc.CallOption) (*ApplyCommandResponse, error)
	GetLogs(ctx context.Context, in *GetLogsRequest, opts ...grpc.CallOption) (*GetLogsResponse, error)
	CommitOperation(ctx context.Context, in *CommitTransaction, opts ...grpc.CallOption) (*CommitOperationResponse, error)
	ApplyOperation(ctx context.Context, in *ApplyOperationRequest, opts ...grpc.CallOption) (*ApplyOperationResponse, error)
	ForwardOperation(ctx context.Context, in *ForwardOperationRequest, opts ...grpc.CallOption) (*ForwardOperationResponse, error)
}

type raftServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewRaftServiceClient(cc grpc.ClientConnInterface) RaftServiceClient {
	return &raftServiceClient{cc}
}

func (c *raftServiceClient) AddReplica(ctx context.Context, in *AddrInfo, opts ...grpc.CallOption) (*AddrInfoStatus, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(AddrInfoStatus)
	err := c.cc.Invoke(ctx, RaftService_AddReplica_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) AppendEntries(ctx context.Context, in *AppendEntriesRequest, opts ...grpc.CallOption) (*AppendEntriesResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(AppendEntriesResponse)
	err := c.cc.Invoke(ctx, RaftService_AppendEntries_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) RequestVote(ctx context.Context, in *RequestVoteRequest, opts ...grpc.CallOption) (*RequestVoteResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(RequestVoteResponse)
	err := c.cc.Invoke(ctx, RaftService_RequestVote_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) ApplyCommand(ctx context.Context, in *ApplyCommandRequest, opts ...grpc.CallOption) (*ApplyCommandResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ApplyCommandResponse)
	err := c.cc.Invoke(ctx, RaftService_ApplyCommand_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) GetLogs(ctx context.Context, in *GetLogsRequest, opts ...grpc.CallOption) (*GetLogsResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(GetLogsResponse)
	err := c.cc.Invoke(ctx, RaftService_GetLogs_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) CommitOperation(ctx context.Context, in *CommitTransaction, opts ...grpc.CallOption) (*CommitOperationResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(CommitOperationResponse)
	err := c.cc.Invoke(ctx, RaftService_CommitOperation_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) ApplyOperation(ctx context.Context, in *ApplyOperationRequest, opts ...grpc.CallOption) (*ApplyOperationResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ApplyOperationResponse)
	err := c.cc.Invoke(ctx, RaftService_ApplyOperation_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *raftServiceClient) ForwardOperation(ctx context.Context, in *ForwardOperationRequest, opts ...grpc.CallOption) (*ForwardOperationResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ForwardOperationResponse)
	err := c.cc.Invoke(ctx, RaftService_ForwardOperation_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// RaftServiceServer is the server API for RaftService service.
// All implementations must embed UnimplementedRaftServiceServer
// for forward compatibility.
//
// Raft service definition
type RaftServiceServer interface {
	AddReplica(context.Context, *AddrInfo) (*AddrInfoStatus, error)
	AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error)
	RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error)
	ApplyCommand(context.Context, *ApplyCommandRequest) (*ApplyCommandResponse, error)
	GetLogs(context.Context, *GetLogsRequest) (*GetLogsResponse, error)
	CommitOperation(context.Context, *CommitTransaction) (*CommitOperationResponse, error)
	ApplyOperation(context.Context, *ApplyOperationRequest) (*ApplyOperationResponse, error)
	ForwardOperation(context.Context, *ForwardOperationRequest) (*ForwardOperationResponse, error)
	mustEmbedUnimplementedRaftServiceServer()
}

// UnimplementedRaftServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedRaftServiceServer struct{}

func (UnimplementedRaftServiceServer) AddReplica(context.Context, *AddrInfo) (*AddrInfoStatus, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddReplica not implemented")
}
func (UnimplementedRaftServiceServer) AppendEntries(context.Context, *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AppendEntries not implemented")
}
func (UnimplementedRaftServiceServer) RequestVote(context.Context, *RequestVoteRequest) (*RequestVoteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RequestVote not implemented")
}
func (UnimplementedRaftServiceServer) ApplyCommand(context.Context, *ApplyCommandRequest) (*ApplyCommandResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ApplyCommand not implemented")
}
func (UnimplementedRaftServiceServer) GetLogs(context.Context, *GetLogsRequest) (*GetLogsResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetLogs not implemented")
}
func (UnimplementedRaftServiceServer) CommitOperation(context.Context, *CommitTransaction) (*CommitOperationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CommitOperation not implemented")
}
func (UnimplementedRaftServiceServer) ApplyOperation(context.Context, *ApplyOperationRequest) (*ApplyOperationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ApplyOperation not implemented")
}
func (UnimplementedRaftServiceServer) ForwardOperation(context.Context, *ForwardOperationRequest) (*ForwardOperationResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method ForwardOperation not implemented")
}
func (UnimplementedRaftServiceServer) mustEmbedUnimplementedRaftServiceServer() {}
func (UnimplementedRaftServiceServer) testEmbeddedByValue()                     {}

// UnsafeRaftServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to RaftServiceServer will
// result in compilation errors.
type UnsafeRaftServiceServer interface {
	mustEmbedUnimplementedRaftServiceServer()
}

func RegisterRaftServiceServer(s grpc.ServiceRegistrar, srv RaftServiceServer) {
	// If the following call pancis, it indicates UnimplementedRaftServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&RaftService_ServiceDesc, srv)
}

func _RaftService_AddReplica_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddrInfo)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).AddReplica(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RaftService_AddReplica_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).AddReplica(ctx, req.(*AddrInfo))
	}
	return interceptor(ctx, in, info, handler)
}

func _RaftService_AppendEntries_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AppendEntriesRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).AppendEntries(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RaftService_AppendEntries_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).AppendEntries(ctx, req.(*AppendEntriesRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RaftService_RequestVote_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RequestVoteRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).RequestVote(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RaftService_RequestVote_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).RequestVote(ctx, req.(*RequestVoteRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RaftService_ApplyCommand_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ApplyCommandRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).ApplyCommand(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RaftService_ApplyCommand_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).ApplyCommand(ctx, req.(*ApplyCommandRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RaftService_GetLogs_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetLogsRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).GetLogs(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RaftService_GetLogs_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).GetLogs(ctx, req.(*GetLogsRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RaftService_CommitOperation_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CommitTransaction)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).CommitOperation(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RaftService_CommitOperation_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).CommitOperation(ctx, req.(*CommitTransaction))
	}
	return interceptor(ctx, in, info, handler)
}

func _RaftService_ApplyOperation_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ApplyOperationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).ApplyOperation(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RaftService_ApplyOperation_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).ApplyOperation(ctx, req.(*ApplyOperationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _RaftService_ForwardOperation_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ForwardOperationRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(RaftServiceServer).ForwardOperation(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: RaftService_ForwardOperation_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(RaftServiceServer).ForwardOperation(ctx, req.(*ForwardOperationRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// RaftService_ServiceDesc is the grpc.ServiceDesc for RaftService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var RaftService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "proto.RaftService",
	HandlerType: (*RaftServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "addReplica",
			Handler:    _RaftService_AddReplica_Handler,
		},
		{
			MethodName: "AppendEntries",
			Handler:    _RaftService_AppendEntries_Handler,
		},
		{
			MethodName: "RequestVote",
			Handler:    _RaftService_RequestVote_Handler,
		},
		{
			MethodName: "ApplyCommand",
			Handler:    _RaftService_ApplyCommand_Handler,
		},
		{
			MethodName: "GetLogs",
			Handler:    _RaftService_GetLogs_Handler,
		},
		{
			MethodName: "CommitOperation",
			Handler:    _RaftService_CommitOperation_Handler,
		},
		{
			MethodName: "ApplyOperation",
			Handler:    _RaftService_ApplyOperation_Handler,
		},
		{
			MethodName: "ForwardOperation",
			Handler:    _RaftService_ForwardOperation_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "raft/proto/raft.proto",
}
