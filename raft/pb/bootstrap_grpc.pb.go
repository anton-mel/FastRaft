// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.28.3
// source: raft/proto/bootstrap.proto

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
	BootstrapService_AddReplica_FullMethodName = "/proto.BootstrapService/addReplica"
)

// BootstrapServiceClient is the client API for BootstrapService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type BootstrapServiceClient interface {
	AddReplica(ctx context.Context, in *AddrInfo, opts ...grpc.CallOption) (*AddrInfoStatus, error)
}

type bootstrapServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewBootstrapServiceClient(cc grpc.ClientConnInterface) BootstrapServiceClient {
	return &bootstrapServiceClient{cc}
}

func (c *bootstrapServiceClient) AddReplica(ctx context.Context, in *AddrInfo, opts ...grpc.CallOption) (*AddrInfoStatus, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(AddrInfoStatus)
	err := c.cc.Invoke(ctx, BootstrapService_AddReplica_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// BootstrapServiceServer is the server API for BootstrapService service.
// All implementations must embed UnimplementedBootstrapServiceServer
// for forward compatibility.
type BootstrapServiceServer interface {
	AddReplica(context.Context, *AddrInfo) (*AddrInfoStatus, error)
	mustEmbedUnimplementedBootstrapServiceServer()
}

// UnimplementedBootstrapServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedBootstrapServiceServer struct{}

func (UnimplementedBootstrapServiceServer) AddReplica(context.Context, *AddrInfo) (*AddrInfoStatus, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddReplica not implemented")
}
func (UnimplementedBootstrapServiceServer) mustEmbedUnimplementedBootstrapServiceServer() {}
func (UnimplementedBootstrapServiceServer) testEmbeddedByValue()                          {}

// UnsafeBootstrapServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to BootstrapServiceServer will
// result in compilation errors.
type UnsafeBootstrapServiceServer interface {
	mustEmbedUnimplementedBootstrapServiceServer()
}

func RegisterBootstrapServiceServer(s grpc.ServiceRegistrar, srv BootstrapServiceServer) {
	// If the following call pancis, it indicates UnimplementedBootstrapServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&BootstrapService_ServiceDesc, srv)
}

func _BootstrapService_AddReplica_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddrInfo)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(BootstrapServiceServer).AddReplica(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: BootstrapService_AddReplica_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(BootstrapServiceServer).AddReplica(ctx, req.(*AddrInfo))
	}
	return interceptor(ctx, in, info, handler)
}

// BootstrapService_ServiceDesc is the grpc.ServiceDesc for BootstrapService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var BootstrapService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "proto.BootstrapService",
	HandlerType: (*BootstrapServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "addReplica",
			Handler:    _BootstrapService_AddReplica_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "raft/proto/bootstrap.proto",
}
