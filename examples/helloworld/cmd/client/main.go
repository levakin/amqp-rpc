package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/levakin/amqp-rpc/examples/helloworld/proto"
	"github.com/levakin/amqp-rpc/rpc"
)

func main() {
	if err := run(); err != nil {
		log.Fatalln(err)
	}
}

func run() error {
	timeout := time.Second * 10
	rpcClient, err := rpc.NewClient("amqp://guest:guest@localhost/", "rpc", 1, 20, 1, timeout, true, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := rpcClient.Close(); err != nil {
			log.Println(err)
		}
	}()

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
