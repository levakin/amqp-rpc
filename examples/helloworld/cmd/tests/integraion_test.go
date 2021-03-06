package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/levakin/amqp-rpc/codes"
	"github.com/levakin/amqp-rpc/examples/helloworld/proto"
	"github.com/levakin/amqp-rpc/health"
	"github.com/levakin/amqp-rpc/health/healthpb"
	"github.com/levakin/amqp-rpc/internal/logging"
	"github.com/levakin/amqp-rpc/rabbitmq"
	"github.com/levakin/amqp-rpc/rpc"
	"github.com/levakin/amqp-rpc/status"
)

const (
	rpcCallTimeout = time.Second * 6
	amqpAddr       = "amqp://guest:guest@localhost:5672/"
	errMsg         = "test error message"
)

func TestSendRequest(t *testing.T) {
	logging.ConfigureLogger()

	producerPool, err := rabbitmq.NewPool(amqpAddr, "producer_pool")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := producerPool.Close(); err != nil {
			log.Println(err)
		}
	}()

	consumerPool, err := rabbitmq.NewPool(amqpAddr, "consumer_pool")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := consumerPool.Close(); err != nil {
			log.Println(err)
		}
	}()

	rpcServerQueueName := "rpc." + uuid.New().String()

	rpcServer, err := rpc.NewRabbitMQServer(producerPool, consumerPool, rpcServerQueueName)
	if err != nil {
		t.Fatal(err)
	}

	proto.RegisterGreeterServer(rpcServer, &server{})

	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- rpcServer.Serve(serveCtx)
	}()

	rpcClient, err := rpc.NewClient(
		producerPool,
		consumerPool,
		rpcServerQueueName,
		rpc.WithClientCallTimeout(rpcCallTimeout),
	)
	if err != nil {
		t.Fatal(err)
	}

	serveCallbacksErrCh := make(chan error, 1)
	go func() {
		serveCallbacksErrCh <- rpcClient.ServeCallbacks(serveCtx)
	}()

	greeterClient := proto.NewGreeterClient(rpcClient)

	name := "John Cena"
	for i := 0; i < 10; i++ {
		req := proto.HelloRequest{Name: fmt.Sprintf("%s_%d", name, i)}
		resp, err := greeterClient.SayHello(context.Background(), &req)
		if err != nil {
			t.Error(err)
		}

		want := fmt.Sprintf("hello %s_%d", name, i)
		if resp.GetMessage() != want {
			t.Error("want " + want + " got " + resp.GetMessage())
		}
		log.Infof("Want is: %s", want)
	}

	cancel()

	if err := <-serveCallbacksErrCh; err != nil {
		t.Errorf("err server callbacks: %v", err)
	}

	if err := <-serverErrCh; err != nil {
		t.Errorf("err server: %v", err)
	}
}

func TestWantError(t *testing.T) {
	producerPool, err := rabbitmq.NewPool(amqpAddr, "producer_pool")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := producerPool.Close(); err != nil {
			log.Println(err)
		}
	}()

	consumerPool, err := rabbitmq.NewPool(amqpAddr, "consumer_pool")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := consumerPool.Close(); err != nil {
			log.Println(err)
		}
	}()

	rpcServerQueueName := "rpc." + uuid.New().String()

	rpcServer, err := rpc.NewRabbitMQServer(producerPool, consumerPool, rpcServerQueueName)
	if err != nil {
		t.Fatal(err)
	}

	proto.RegisterGreeterServer(rpcServer, &server{})

	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- rpcServer.Serve(serveCtx)
	}()

	rpcClient, err := rpc.NewClient(
		producerPool,
		consumerPool,
		rpcServerQueueName,
		rpc.WithClientCallTimeout(rpcCallTimeout),
	)
	if err != nil {
		t.Fatal(err)
	}

	serveCallbacksErrCh := make(chan error, 1)
	go func() {
		serveCallbacksErrCh <- rpcClient.ServeCallbacks(serveCtx)
	}()

	greeterClient := proto.NewGreeterClient(rpcClient)

	_, err = greeterClient.ReturnErr(context.Background(), &proto.ReturnErrRequest{Message: errMsg})
	if err == nil {
		t.Fatal("want error, got nil")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Error("want status, got nothing")
	}

	if st.Code() != codes.Internal {
		t.Errorf("want code Internal, got %d", st.Code())
	}

	if st.Message() != errMsg {
		t.Errorf("want message %q, got %q", errMsg, st.Message())
	}

	if len(st.Details()) != 2 {
		t.Errorf("want 2 detail, got %d", len(st.Details()))
	}

	_, ok = st.Details()[0].(*emptypb.Empty)
	if !ok {
		t.Errorf("want empty, got %T", st.Details()[0])
	}

	dt, ok := st.Details()[1].(*proto.SomeErrorDetails)
	if !ok {
		t.Errorf("want some error details, got %T", st.Details()[0])
	}

	if errMsg != dt.GetInfo() {
		t.Errorf("want %s, got %s", errMsg, dt.GetInfo())
	}

	cancel()
	if err := <-serveCallbacksErrCh; err != nil {
		t.Errorf("err server callbacks: %v", err)
	}

	if err := <-serverErrCh; err != nil {
		t.Errorf("err server: %v", err)
	}
}

func TestHealthServer(t *testing.T) {
	rpcServerQueueName := "rpc." + uuid.New().String()

	producerPool, err := rabbitmq.NewPool(amqpAddr, "producer_pool")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := producerPool.Close(); err != nil {
			log.Println(err)
		}
	}()

	consumerPool, err := rabbitmq.NewPool(amqpAddr, "consumer_pool")
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := consumerPool.Close(); err != nil {
			log.Println(err)
		}
	}()

	rpcServer, err := rpc.NewRabbitMQServer(producerPool, consumerPool, rpcServerQueueName)
	if err != nil {
		t.Fatal(err)
	}
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	proto.RegisterGreeterServer(rpcServer, &server{})
	healthpb.RegisterHealthServer(rpcServer, healthServer)

	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- rpcServer.Serve(serveCtx)
	}()

	rpcClient, err := rpc.NewClient(
		producerPool,
		consumerPool,
		rpcServerQueueName,
		rpc.WithClientCallTimeout(rpcCallTimeout),
	)
	if err != nil {
		t.Fatal(err)
	}

	serveCallbacksErrCh := make(chan error, 1)
	go func() {
		serveCallbacksErrCh <- rpcClient.ServeCallbacks(serveCtx)
	}()

	hc := healthpb.NewHealthClient(rpcClient)

	hcResp, err := hc.Check(context.Background(), &healthpb.HealthCheckRequest{Service: ""})
	if err != nil {
		t.Error(err)
	}

	if hcResp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("want %s got %s", healthpb.HealthCheckResponse_SERVING, hcResp.GetStatus())
	}

	cancel()
	if err := <-serveCallbacksErrCh; err != nil {
		t.Errorf("err server callbacks: %v", err)
	}

	if err := <-serverErrCh; err != nil {
		t.Errorf("err server: %v", err)
	}
}

type server struct {
	proto.UnimplementedGreeterServer
}

func (s *server) SayHello(ctx context.Context, hr *proto.HelloRequest) (*proto.HelloReply, error) {
	return &proto.HelloReply{Message: "hello " + hr.GetName()}, nil
}

func (s *server) ReturnErr(ctx context.Context, hr *proto.ReturnErrRequest) (*emptypb.Empty, error) {
	st := status.New(codes.Internal, errMsg)
	st, err := st.WithDetails(&emptypb.Empty{}, &proto.SomeErrorDetails{
		Info: errMsg,
	})
	if err != nil {
		return nil, err
	}

	return nil, st.Err()
}
