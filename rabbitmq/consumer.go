package rabbitmq

import (
	"context"
	"time"

	"github.com/streadway/amqp"
)

type DeliveryHandler func(ctx context.Context, delivery amqp.Delivery)

type Consumer struct {
	queueName string
	client    *Client
	handler   DeliveryHandler
}

func NewConsumer(queueName string, client *Client, handler DeliveryHandler) *Consumer {
	return &Consumer{
		queueName: queueName,
		client:    client,
		handler:   handler,
	}
}

func (c *Consumer) Close() error {
	return c.client.Close()
}

// Consume starts consuming messages
func (c *Consumer) Consume(ctx context.Context) error {
	for {
		if !c.client.alive {
			return ErrClientIsNotAlive
		}

		// wait for connection
		if !c.client.isConnected {
			for {
				if !c.client.alive {
					return ErrClientIsNotAlive
				}

				if c.client.isConnected {
					break
				}
				time.Sleep(1 * time.Second)
			}
		}

		err := c.client.Channel.Qos(1, 0, false)
		if err != nil {
			return err
		}

		msgs, err := c.client.Channel.Consume(
			c.queueName,
			consumerName(0), // Consumer
			false,           // Auto-Ack
			false,           // Exclusive
			false,           // No-local
			false,           // No-Wait
			nil,             // Args
		)
		if err != nil {
			return err
		}

	HandleDeliveries:
		for {
			select {
			case msg, ok := <-msgs:
				if !ok {
					break HandleDeliveries
				}
				c.handler(ctx, msg)
			case <-ctx.Done():
				return nil
			}
		}
	}
}
