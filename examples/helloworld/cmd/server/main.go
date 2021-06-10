package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/levakin/amqp-rpc/examples/helloworld/proto"
	"github.com/levakin/amqp-rpc/internal/logging"
	"github.com/levakin/amqp-rpc/rpc"
)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() error {
	logging.ConfigureLogger()
	rpcServer, err := rpc.NewRabbitMqServer("amqp://guest:guest@localhost:5672/", "rpc", 1, 20, 1, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := rpcServer.Close(); err != nil {
			log.Errorln(err)
		}
	}()

	proto.RegisterGreeterServer(rpcServer, &server{})

	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverErrCh := make(chan error, 1)
	go func() {
		serverErrCh <- rpcServer.Serve(serveCtx)
	}()

	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrCh:
		if err != nil {
			return errors.Wrap(err, "before shutdown signal")
		}
	case <-shutdown:
		cancel()

		if err := <-serverErrCh; err != nil {
			return errors.Wrap(err, "during shutdown")
		}
	}

	return nil
}

type server struct {
	proto.UnimplementedGreeterServer
}

func (s *server) SayHello(context.Context, *proto.HelloRequest) (*proto.HelloReply, error) {
	return &proto.HelloReply{Message: "hello"}, nil
}
