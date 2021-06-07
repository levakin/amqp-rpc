package rabbitmq

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

var (
	// ErrDisconnected represents an error when client is disconnected.
	// When this error occurs, handleReconnect will try to reconnect
	ErrDisconnected     = errors.New("disconnected from rabbitmq")
	ErrClientIsNotAlive = errors.New("client is not alive")
)

const (
	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second

	// When resending messages the server didn't confirm
	resendDelay = 5 * time.Second
)

// Client holds necessary information for rabbitMQ
type Client struct {
	connection            *amqp.Connection
	Channel               *amqp.Channel
	stopHandlingReconnect chan struct{}
	notifyClose           chan *amqp.Error
	notifyConfirm         chan amqp.Confirmation
	isConnected           bool
	alive                 bool
}

// NewClient is a constructor that takes address queue name, logger.
// We create the client, and start the connection process.
func NewClient(addr string, tlsCfg *tls.Config) (*Client, error) {
	stopHandlingReconnect := make(chan struct{})
	client := Client{
		stopHandlingReconnect: stopHandlingReconnect,
		alive:                 true,
	}

	if err := client.connect(addr, tlsCfg); err != nil {
		return nil, errors.Wrapf(err, "could not connect to amqp: %s", addr)
	}

	go client.handleReconnect(addr, tlsCfg)

	return &client, nil
}

// handleReconnect will wait for a connection error on
// notifyClose, and then continuously attempt to reconnect.
func (c *Client) handleReconnect(addr string, tlsCfg *tls.Config) {
	for c.alive {
		if !c.isConnected {
			var retryCount int
			for {
				if err := c.connect(addr, tlsCfg); err == nil {
					// Connected to RabbitMQ successfully
					// Stop retrying
					break
				}

				if !c.alive {
					// Client was closed
					return
				}

				select {
				case <-c.stopHandlingReconnect:
					return
				case <-time.After(reconnectDelay + time.Duration(retryCount)*time.Second):
					retryCount++
				}
			}
		}

		select {
		case <-c.stopHandlingReconnect:
			return
		case <-c.notifyClose:
		}
	}
}

// connect will make a single attempt to connect to
// RabbitMq. It returns the success of the attempt.
func (c *Client) connect(addr string, tlsCfg *tls.Config) error {
	conn, err := amqp.DialTLS(addr, tlsCfg)
	if err != nil {
		return errors.Wrap(err, "failed to dial rabbitMQ server")
	}

	ch, err := conn.Channel()
	if err != nil {
		return errors.Wrap(err, "failed connecting to channel")
	}

	if err := ch.Confirm(false); err != nil {
		return errors.Wrap(err, "failed to confirm")
	}

	c.changeConnection(conn, ch)

	c.isConnected = true

	return nil
}

// changeConnection takes a new connection to the queue,
// and updates the Channel listeners to reflect this.
func (c *Client) changeConnection(connection *amqp.Connection, channel *amqp.Channel) {
	c.connection = connection
	c.Channel = channel
	c.notifyClose = make(chan *amqp.Error)
	c.notifyConfirm = make(chan amqp.Confirmation)
	c.Channel.NotifyClose(c.notifyClose)
	c.Channel.NotifyPublish(c.notifyConfirm)
}

// Close gracefully stops connection
func (c *Client) Close() error {
	if !c.isConnected {
		return nil
	}
	c.alive = false
	// Waiting for current messages to be processed...

	// stop handling reconnect
	c.stopHandlingReconnect <- struct{}{}

	// Closing consumer
	err := c.Channel.Cancel(consumerName(0), false)
	if err != nil {
		return errors.Wrapf(err, "error canceling consumer %s", consumerName(0))
	}

	err = c.Channel.Close()
	if err != nil {
		return err
	}

	err = c.connection.Close()
	if err != nil {
		return err
	}

	c.isConnected = false
	return nil
}

func consumerName(i int) string {
	return fmt.Sprintf("go-consumer-%v", i)
}
