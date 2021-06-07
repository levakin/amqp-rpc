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
	rpcClient, err := rpc.NewClient("amqp://guest:guest@localhost/", "rpc", time.Second*10, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := rpcClient.Close(); err != nil {
			log.Println(err)
		}
	}()
	go func() {
		if err := rpcClient.HandleCallbacks(context.Background()); err != nil {
			log.Println(err)
		}
	}()
	greeterClient := proto.NewGreeterClient(rpcClient)
	hr, err := greeterClient.SayHello(context.Background(), &proto.HelloRequest{
		Name: "John Cena",
	})
	if err != nil {
		return err
	}

	fmt.Println(hr.String())

	return nil
}
