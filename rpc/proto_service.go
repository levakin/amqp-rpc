package rpc

import "context"

const ProtobufContentType = "application/proto"

type methodHandler func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor UnaryServerInterceptor) (interface{}, error)

// ServiceDesc represents an RPC service's specification.
type ServiceDesc struct {
	ServiceName string
	// The pointer to the service interface. Used to check whether the user
	// provided implementation satisfies the interface requirements.
	HandlerType interface{}
	Methods     []MethodDesc
	Metadata    interface{}
}

// ServiceInfo wraps information about a service. It is very similar to
// ServiceInfo and is constructed from it for internal purposes.
type ServiceInfo struct {
	// Contains the implementation for the methods in this service.
	ServiceImpl interface{}
	Methods     map[string]*MethodDesc
	Metadata    interface{}
}

// ServiceRegistrar wraps a single method that supports service registration. It
// enables users to pass concrete types other than amqp-rpc.Server to the service
// registration methods exported by the IDL generated code.
type ServiceRegistrar interface {
	// RegisterProtoService registers a service and its implementation to the
	// concrete type implementing this interface.  It may not be called
	// once the server has started serving.
	// desc describes the service and its methods and handlers. impl is the
	// service implementation which is passed to the method handlers.
	RegisterProtoService(desc *ServiceDesc, impl interface{})
}

// MethodDesc represents an RPC service's method specification.
type MethodDesc struct {
	MethodName string
	Handler    methodHandler
}

// UnaryServerInfo consists of various information about a unary RPC on
// server side. All per-rpc information may be mutated by the interceptor.
type UnaryServerInfo struct {
	// Server is the service implementation the user provides. This is read-only.
	Server interface{}
	// FullMethod is the full RPC method string, i.e., /package.service/method.
	FullMethod string
}

// UnaryHandler defines the handler invoked by UnaryServerInterceptor to complete the normal
// execution of a unary RPC. If a UnaryHandler returns an error, it should be produced by the
// status package, or else RPC will use codes.Unknown as the status code and err.Error() as
// the status message of the RPC.
type UnaryHandler func(ctx context.Context, req interface{}) (interface{}, error)

// UnaryServerInterceptor provides a hook to intercept the execution of a unary RPC on the server. info
// contains all the information of this RPC the interceptor can operate on. And handler is the wrapper
// of the service method implementation. It is the responsibility of the interceptor to invoke handler
// to complete the RPC.
type UnaryServerInterceptor func(ctx context.Context, req interface{}, info *UnaryServerInfo, handler UnaryHandler) (resp interface{}, err error)
