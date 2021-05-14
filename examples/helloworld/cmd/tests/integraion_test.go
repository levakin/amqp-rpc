package tests

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/levakin/amqp-rpc/codes"
	"github.com/levakin/amqp-rpc/examples/helloworld/proto"
	"github.com/levakin/amqp-rpc/rabbitmq"
	"github.com/levakin/amqp-rpc/rpc"
	"github.com/levakin/amqp-rpc/status"
)

const (
	rpcTimeout = time.Second * 6
	amqpAddr   = "amqp://guest:guest@localhost/"
	errMsg     = "test error message"
)

func TestSendRequest(t *testing.T) {
	rpcServerQueueName := "rpc." + uuid.New().String()

	rpcServer, err := rpc.NewServer(log.Default(), rpcServerQueueName, amqpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rpcServer.Close(); err != nil {
			t.Error("error closing rpc server:", err)
		}
	}()

	proto.RegisterGreeterServer(rpcServer, &server{})

	go func() {
		if err := rpcServer.Serve(context.Background()); err != nil {
			if err != rabbitmq.ErrClientIsNotAlive {
				t.Error(err)
			}
			return
		}
	}()

	rpcClient, err := rpc.NewClient(amqpAddr, rpcServerQueueName, rpcTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rpcClient.Close(); err != nil {
			t.Error("error closing rpc server:", err)
		}
	}()
	go func() {
		if err := rpcClient.HandleCallbacks(context.Background()); err != nil {
			if err != rabbitmq.ErrClientIsNotAlive {
				t.Error(err)
			}
			return
		}
	}()

	greeterClient := proto.NewGreeterClient(rpcClient)

	name := "John Cena"
	req := proto.HelloRequest{Name: name}
	for i := 0; i < 10; i++ {
		resp, err := greeterClient.SayHello(context.Background(), &req)
		if err != nil {
			t.Error(err)
		}
		want := "hello " + name
		if resp.GetMessage() != want {
			t.Error("want " + want + " got " + resp.GetMessage())
		}
	}
}

func TestWantError(t *testing.T) {
	rpcServerQueueName := "rpc." + uuid.New().String()

	rpcServer, err := rpc.NewServer(log.Default(), rpcServerQueueName, amqpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rpcServer.Close(); err != nil {
			t.Error("error closing rpc server:", err)
		}
	}()

	proto.RegisterGreeterServer(rpcServer, &server{})

	go func() {
		if err := rpcServer.Serve(context.Background()); err != nil {
			if err != rabbitmq.ErrClientIsNotAlive {
				t.Error(err)
			}
			return
		}
	}()

	rpcClient, err := rpc.NewClient(amqpAddr, rpcServerQueueName, rpcTimeout)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := rpcClient.Close(); err != nil {
			t.Error("error closing rpc server:", err)
		}
	}()
	go func() {
		if err := rpcClient.HandleCallbacks(context.Background()); err != nil {
			if err != rabbitmq.ErrClientIsNotAlive {
				t.Error(err)
			}
			return
		}
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
