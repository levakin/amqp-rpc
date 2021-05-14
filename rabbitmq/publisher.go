package rabbitmq

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type Publisher struct {
	client *Client
}

func NewPublisher(client *Client) *Publisher {
	return &Publisher{client: client}
}

func (p *Publisher) Close() error {
	return p.client.Close()
}

// Publish will push data onto the queue, and wait for a confirmation.
// If no confirms are received until within the resendTimeout,
// it continuously resends messages until a confirmation is received.
// This will block until the server sends a confirm.
func (p *Publisher) Publish(ctx context.Context, pub amqp.Publishing, routingKey string) error {
	for {
		if err := p.UnsafePublish(pub, routingKey); err != nil {
			return errors.Wrap(err, "failed to unsafe publish")
		}

		select {
		case confirm := <-p.client.notifyConfirm:
			if confirm.Ack {
				return nil
			}
		case <-ctx.Done():
			// TODO: maybe return err
			return nil
		case <-time.After(resendDelay):
		}
	}
}

// UnsafePublish will push to the queue without checking for
// confirmation. It returns an error if it fails to connect.
// No guarantees are provided for whether the server will
// receive the message.
func (p *Publisher) UnsafePublish(pub amqp.Publishing, routingKey string) error {
	if !p.client.alive {
		return ErrClientIsNotAlive
	}

	// wait for connection
	if !p.client.isConnected {
		for {
			if !p.client.alive {
				return ErrClientIsNotAlive
			}

			if p.client.isConnected {
				break
			}
			time.Sleep(1 * time.Second)
		}
	}

	return p.client.Channel.Publish(
		"",         // Exchange
		routingKey, // Routing key
		false,      // Mandatory
		false,      // Immediate
		pub,
	)
}
