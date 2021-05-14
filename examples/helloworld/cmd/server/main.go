package main

import (
	"context"
	"log"

	"github.com/levakin/amqp-rpc/v0/examples/helloworld/proto"
	"github.com/levakin/amqp-rpc/v0/rpc"
)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() error {
	rpcServer, err := rpc.NewServer(log.Default(), "rpc", "amqp://guest:guest@localhost/")
	if err != nil {
		return err
	}
	defer func() {
		if err := rpcServer.Close(); err != nil {
			log.Println(err)
		}
	}()

	proto.RegisterGreeterServer(rpcServer, &server{})

	if err := rpcServer.Serve(context.Background()); err != nil {
		return err
	}

	return nil
}

type server struct {
	proto.UnimplementedGreeterServer
}

func (s *server) SayHello(context.Context, *proto.HelloRequest) (*proto.HelloReply, error) {
	return &proto.HelloReply{Message: "hello"}, nil
}
