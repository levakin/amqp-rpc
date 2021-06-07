package rpc

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"

	"github.com/levakin/amqp-rpc/codes"
	"github.com/levakin/amqp-rpc/rabbitmq"
	"github.com/levakin/amqp-rpc/status"
)

// serviceInfo wraps information about a service. It is very similar to
// ServiceDesc and is constructed from it for internal purposes.
type serviceInfo struct {
	// Contains the implementation for the methods in this service.
	serviceImpl interface{}
	methods     map[string]*MethodDesc
	mdata       interface{}
}

// Server implements ServiceRegistrar
type Server struct {
	publisher *rabbitmq.Publisher
	consumer  *rabbitmq.Consumer

	amqpAddr string
	queue    string
	serve    bool
	services map[string]*serviceInfo // service name -> service info
	logger   *log.Logger

	mu sync.Mutex
}

// NewServer returns AMQP RPC server instance
func NewServer(logger *log.Logger, queue, amqpAddr string, tlsCfg *tls.Config) (*Server, error) {
	pubRMQClient, err := rabbitmq.NewClient(amqpAddr, tlsCfg)
	if err != nil {
		return nil, err
	}

	s := Server{
		publisher: rabbitmq.NewPublisher(pubRMQClient),
		amqpAddr:  amqpAddr,
		queue:     queue,
		services:  make(map[string]*serviceInfo),
		logger:    logger,
	}

	conRMQClient, err := rabbitmq.NewClient(amqpAddr, tlsCfg)
	if err != nil {
		return nil, err
	}

	// Declare server queue
	if _, err := conRMQClient.Channel.QueueDeclare(
		queue,
		false, // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	); err != nil {
		return nil, err
	}

	// TODO: implement worker pool pattern. Make consumer for each worker.
	c := rabbitmq.NewConsumer(queue, conRMQClient, s.handleDelivery)
	s.consumer = c

	return &s, nil
}

func (s *Server) Close() error {
	if err := s.consumer.Close(); err != nil {
		return err
	}

	if err := s.publisher.Close(); err != nil {
		return err
	}

	return nil
}

// RegisterService registers a service to server
func (s *Server) RegisterService(sd *ServiceDesc, impl interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.logger.Printf("RegisterService(%q)\n", sd.ServiceName)
	if s.serve {
		s.logger.Fatalf("amqp-rpc: Server.RegisterService after Server.Serve for %q\n", sd.ServiceName)
	}

	if _, ok := s.services[sd.ServiceName]; ok {
		s.logger.Fatalf("amqp-rpc: Server.RegisterService found duplicate service registration for %q\n", sd.ServiceName)
	}

	info := &serviceInfo{
		serviceImpl: impl,
		methods:     make(map[string]*MethodDesc),
		mdata:       sd.Metadata,
	}

	for i := range sd.Methods {
		d := &sd.Methods[i]
		info.methods[d.MethodName] = d
	}
	s.services[sd.ServiceName] = info
}

// Serve starts the server
func (s *Server) Serve(ctx context.Context) error {
	s.serve = true

	if err := s.consumer.Consume(ctx); err != nil {
		return err
	}

	return nil
}

func (s *Server) handleDelivery(ctx context.Context, delivery amqp.Delivery) {
	// TODO: decide if it is really needed to recover from panic
	// defer func(m amqp.Delivery) {
	// 	if err := recover(); err != nil {
	// 		stack := make([]byte, 8096)
	// 		stack = stack[:runtime.Stack(stack, false)]
	// 		log.Printf("panic recovery for rabbitMQ message: %v: stack %v\n", err, stack)
	//
	// 		if err := delivery.Nack(false, false); err != nil {
	// 			log.Printf("failed to nack message: %v\n", err)
	// 		}
	// 	}
	// }(delivery)

	if err := s.processUnaryRPC(ctx, delivery); err != nil {
		fmt.Println("unhandled error during processing unary RPC:", err) // TODO: remove this
		if err := delivery.Nack(false, false); err != nil {
			log.Printf("failed to nack message: %v\n", err)
		}
		return
	}

	if err := delivery.Ack(false); err != nil {
		log.Printf("failed to ack message: %v\n", err)
	}
}

func (s *Server) processUnaryRPC(ctx context.Context, delivery amqp.Delivery) error {
	fullMethod := delivery.Headers["fullMethod"].(string)
	splittedMethod := strings.Split(fullMethod, "/")

	reqService := splittedMethod[1]
	reqMethod := splittedMethod[2]

	si, ok := s.services[reqService]
	if !ok {
		return errors.New("no such service: " + reqService)
	}

	md, ok := si.methods[reqMethod]
	if !ok {
		return errors.New("no such method: " + reqMethod)
	}

	dec := func(i interface{}) error {
		if err := proto.Unmarshal(delivery.Body, i.(proto.Message)); err != nil {
			return err
		}
		return nil
	}

	reply, appErr := md.Handler(si.serviceImpl, ctx, dec, nil)
	if appErr != nil {
		appStatus, ok := status.FromError(appErr)
		if !ok {
			// Convert appErr if it is not a amqp rpc status error.
			appErr = status.Error(codes.Unknown, appErr.Error())
			appStatus, _ = status.FromError(appErr)
		}

		if err := s.sendStatus(ctx, delivery, appStatus); err != nil {
			return err
		}

		return nil
	}

	replyData, err := proto.Marshal(reply.(proto.Message))
	if err != nil {
		return err
	}

	if err := s.sendResponse(ctx, delivery, replyData); err != nil {
		return err
	}

	return nil
}

func (s *Server) sendStatus(ctx context.Context, delivery amqp.Delivery, st *status.Status) error {
	// Write status code
	h := map[string]interface{}{
		"Amqp-Rpc-Status": fmt.Sprintf("%d", st.Code()),
	}

	// Write status message if exists
	if m := st.Message(); m != "" {
		h["Amqp-Rpc-Message"] = m
	}

	// Write status details if exist
	if p := st.Proto(); p != nil && len(p.Details) > 0 {
		stBytes, err := proto.Marshal(p)
		if err != nil {
			return err
		}
		h["Amqp-Rpc-Status-Details-Bin"] = stBytes
	}

	if err := s.publisher.Publish(ctx, amqp.Publishing{
		Headers:       h,
		ContentType:   protobufContentType,
		CorrelationId: delivery.CorrelationId,
	}, delivery.ReplyTo); err != nil {
		return err
	}

	return nil
}

func (s *Server) sendResponse(ctx context.Context, delivery amqp.Delivery, replyData []byte) error {
	if err := s.publisher.Publish(ctx, amqp.Publishing{
		Headers: map[string]interface{}{
			"Amqp-Rpc-Status": fmt.Sprintf("%d", codes.OK),
		},
		ContentType:   protobufContentType,
		CorrelationId: delivery.CorrelationId,
		Body:          replyData,
	}, delivery.ReplyTo); err != nil {
		return err
	}

	return nil
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

type methodHandler func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor UnaryServerInterceptor) (interface{}, error)

// MethodDesc represents an RPC service's method specification.
type MethodDesc struct {
	MethodName string
	Handler    methodHandler
}

// ServiceDesc represents an RPC service's specification.
type ServiceDesc struct {
	ServiceName string
	// The pointer to the service interface. Used to check whether the user
	// provided implementation satisfies the interface requirements.
	HandlerType interface{}
	Methods     []MethodDesc
	Metadata    interface{}
}

// ServiceRegistrar wraps a single method that supports service registration. It
// enables users to pass concrete types other than amqp-rpc.Server to the service
// registration methods exported by the IDL generated code.
type ServiceRegistrar interface {
	// RegisterService registers a service and its implementation to the
	// concrete type implementing this interface.  It may not be called
	// once the server has started serving.
	// desc describes the service and its methods and handlers. impl is the
	// service implementation which is passed to the method handlers.
	RegisterService(desc *ServiceDesc, impl interface{})
}
