package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/levakin/amqp-rpc/codes"
	"github.com/levakin/amqp-rpc/rabbitmq"
	"github.com/levakin/amqp-rpc/status"
)

const protobufContentType = "application/proto"

// ClientConnInterface defines the functions clients need to perform unary RPCs.
// It is implemented by *ClientConn, and is only intended to be referenced by generated code.
type ClientConnInterface interface {
	// Invoke performs a unary RPC and returns after the response is received
	// into reply.
	Invoke(ctx context.Context, method string, args interface{}, reply interface{}) error
}

type pendingCall struct {
	st   *status.Status
	done chan struct{}
	data []byte
}

type calls struct {
	mu  sync.Mutex
	pcs map[string]pendingCall
}

func (c *calls) get(corrID string) (pendingCall, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	pc, ok := c.pcs[corrID]
	return pc, ok
}

func (c *calls) set(corrID string, pc pendingCall) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.pcs[corrID] = pc
}

func (c *calls) delete(corrID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	delete(c.pcs, corrID)
}

// Client represents an RPC service's client
type Client struct {
	publisher         *rabbitmq.Publisher
	consumer          *rabbitmq.Consumer
	callbackQueueName string
	callTimeout       time.Duration
	serverQueueName   string

	mu    sync.Mutex
	calls *calls

	done chan struct{}
}

// NewClient creates a new Client
func NewClient(amqpAddr, serverQueue string, callTimeout time.Duration) (*Client, error) {
	pubRMQClient, err := rabbitmq.NewClient(amqpAddr)
	if err != nil {
		return nil, err
	}

	conRMQClient, err := rabbitmq.NewClient(amqpAddr)
	if err != nil {
		return nil, err
	}

	callbackQueueName := "rpc.callback." + uuid.New().String()

	// Declare callback queue
	if _, err := conRMQClient.Channel.QueueDeclare(
		callbackQueueName,
		false, // Durable
		true,  // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	); err != nil {
		return nil, err
	}

	c := &Client{
		publisher:         rabbitmq.NewPublisher(pubRMQClient),
		callbackQueueName: callbackQueueName,
		serverQueueName:   serverQueue,
		calls:             &calls{pcs: make(map[string]pendingCall)},
		callTimeout:       callTimeout,
		done:              make(chan struct{}, 1),
	}
	c.consumer = rabbitmq.NewConsumer(callbackQueueName, conRMQClient, c.handleCallbackFromServer)

	return c, nil
}

func (c *Client) Close() error {
	if err := c.consumer.Close(); err != nil {
		return err
	}

	if err := c.publisher.Close(); err != nil {
		return err
	}

	return nil
}

// HandleCallbacks consumes callbacks from server
func (c *Client) HandleCallbacks(ctx context.Context) error {
	return c.consumer.Consume(ctx)
}

var _ ClientConnInterface = (*Client)(nil)

// Invoke ...
func (c *Client) Invoke(ctx context.Context, method string, req interface{}, reply interface{}) error {
	request, err := proto.Marshal(req.(proto.Message))
	if err != nil {
		return err
	}

	corrID := newCorrID()

	pc := pendingCall{
		done: make(chan struct{}, 1),
	}
	c.calls.set(corrID, pc)
	defer c.calls.delete(corrID)

	if err := c.publisher.Publish(
		ctx,
		amqp.Publishing{
			Headers: map[string]interface{}{
				"fullMethod": method,
			},
			ContentType:   protobufContentType,
			CorrelationId: corrID,
			ReplyTo:       c.callbackQueueName,
			Body:          request,
			Expiration:    fmt.Sprintf("%d", c.callTimeout),
		},
		c.serverQueueName); err != nil {
		return err
	}

	select {
	case <-pc.done:
		pc, _ := c.calls.get(corrID)

		if err := pc.st.Err(); err != nil {
			return err
		}

		if err := proto.Unmarshal(pc.data, reply.(proto.Message)); err != nil {
			return err
		}

	case <-time.After(c.callTimeout):
		return status.Error(codes.DeadlineExceeded, "call timeout exceeded")
	}

	return nil
}

func (c *Client) handleCallbackFromServer(_ context.Context, d amqp.Delivery) {
	pc, ok := c.calls.get(d.CorrelationId)
	if !ok {
		if err := d.Nack(false, false); err != nil {
			log.Error().Err(err).Msg("failed to nack message")
		}
		return
	}

	stStr, ok := d.Headers["Amqp-Rpc-Status"].(string)
	if !ok {
		if err := d.Nack(false, false); err != nil {
			log.Error().Err(err).Msg("failed to nack message")
		}
		return
	}

	var code codes.Code
	if err := json.Unmarshal([]byte(stStr), &code); err != nil {
		if err := d.Nack(false, false); err != nil {
			log.Error().Err(err).Msg("failed to nack message")
		}
		return
	}
	if code != codes.OK {
		st := spb.Status{Code: int32(code)}
		sdb, ok := d.Headers["Amqp-Rpc-Status-Details-Bin"].([]byte)
		if ok {
			if err := proto.Unmarshal(sdb, &st); err != nil {
				if err := d.Nack(false, false); err != nil {
					log.Error().Err(err).Msg("failed to nack message")
				}
				return
			}
		}

		msg, ok := d.Headers["Amqp-Rpc-Message"].(string)
		if ok {
			st.Message = msg
		}

		pc.st = status.FromProto(&st)
	}

	pc.data = d.Body

	c.calls.set(d.CorrelationId, pc)

	pc.done <- struct{}{}

	if err := d.Ack(false); err != nil {
		log.Error().Err(err).Msg("failed to ack message")
	}
}

func newCorrID() string {
	return uuid.New().String()
}
