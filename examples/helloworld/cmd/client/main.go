package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/pkg/errors"

	"github.com/levakin/amqp-rpc/examples/helloworld/proto"
	"github.com/levakin/amqp-rpc/internal/rabbitmq"
	"github.com/levakin/amqp-rpc/rpc"
)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() error {
	timeout := time.Second * 10

	pool, err := rabbitmq.NewPool("amqp://guest:guest@localhost:5672/", 1, 20, "pool", nil)
	if err != nil {
		return errors.Wrap(err, "failed to init RabbitMQ pool")
	}

	defer func() {
		if err := pool.Close(); err != nil {
			log.Println(err)
		}
	}()

	rpcClient, err := rpc.NewClient(
		pool,
		"rpc",
		rpc.WithClientCallTimeout(timeout),
	)
	if err != nil {
		return err
	}

	serveCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serveCallbacksErrCh := make(chan error, 1)
	go func() {
		serveCallbacksErrCh <- rpcClient.ServeCallbacks(serveCtx)
	}()

	greeterClient := proto.NewGreeterClient(rpcClient)
	hr, err := greeterClient.SayHello(context.Background(), &proto.HelloRequest{
		Name: "John Cena",
	})
	if err != nil {
		return err
	}

	fmt.Println(hr.String())

	cancel()
	if err := <-serveCallbacksErrCh; err != nil {
		return err
	}

	return nil
}
