package rpc

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/streadway/amqp"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/levakin/amqp-rpc/codes"
	"github.com/levakin/amqp-rpc/internal/rabbitmq"
	"github.com/levakin/amqp-rpc/status"
)

// ClientConnInterface defines the functions clients need to perform unary RPCs.
// It is implemented by *ClientConn, and is only intended to be referenced by generated code.
type ClientConnInterface interface {
	// Invoke performs a unary RPC and returns after the response is received
	// into reply.
	Invoke(ctx context.Context, method string, args interface{}, reply interface{}) error
}

type Client struct {
	pool              rabbitmq.Pool
	consumer          *rabbitmq.Consumer
	producer          *rabbitmq.Producer
	callTimeout       time.Duration
	callbackQueueName string
	invokeQueueName   string
	pendingCalls      *calls
	waitReplies       bool
}

func NewClient(connStr, invokeQueueName string, connectionsCount, channelsPoolSize int, callbackWorkers int,
	callTimeout time.Duration, waitReplies bool, tlsCfg *tls.Config) (*Client, error) {

	pool, err := rabbitmq.NewPool(connStr, connectionsCount, channelsPoolSize, "client_pool", tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("error connecting to rabbitmq server. Conn string: %s", connStr)
	}

	producer, err := rabbitmq.NewProducer("client", pool)
	if err != nil {
		return nil, err
	}

	var consumer *rabbitmq.Consumer
	var callbackQueueName string

	if waitReplies {
		callbackQueueName = "rpc.callback." + uuid.New().String()
		consumer, err = rabbitmq.NewConsumer(pool, callbackQueueName, callbackWorkers, "callback_consumer")
		if err != nil {
			return nil, err
		}
	}

	return &Client{
		pool:              pool,
		consumer:          consumer,
		producer:          producer,
		callTimeout:       callTimeout,
		callbackQueueName: callbackQueueName,
		invokeQueueName:   invokeQueueName,
		pendingCalls:      &calls{pcs: make(map[string]pendingCall)},
		waitReplies:       waitReplies,
	}, nil
}

// ServeCallbacks consumes callbacks from server
func (c *Client) ServeCallbacks(ctx context.Context) error {
	if !c.waitReplies {
		return fmt.Errorf("cant start serve callbacks while waitReplies is false")
	}
	return c.consumer.Serve(ctx, rabbitmq.DeliveryHandlerFunc(c.handleCallbackFromServer))
}

func (c *Client) Invoke(ctx context.Context, method string, req interface{}, reply interface{}) error {
	requestModel, err := proto.Marshal(req.(proto.Message))
	if err != nil {
		return err
	}

	corrID := newCorrID()

	pc := pendingCall{
		done: make(chan struct{}, 1),
	}
	c.pendingCalls.set(corrID, pc)
	defer c.pendingCalls.delete(corrID)

	pub := amqp.Publishing{
		Headers: map[string]interface{}{
			"fullMethod": method,
		},
		ContentType:   ProtobufContentType,
		CorrelationId: corrID,
		ReplyTo:       c.callbackQueueName,
		Body:          requestModel,
		Expiration:    fmt.Sprintf("%d", c.callTimeout),
	}

	if err := c.producer.Publish(ctx, pub, "", c.invokeQueueName); err != nil {
		return err
	}
	if c.waitReplies {
		select {
		case <-pc.done:
			pc, _ := c.pendingCalls.get(corrID)

			if err := pc.st.Err(); err != nil {
				return err
			}

			if err := proto.Unmarshal(pc.data, reply.(proto.Message)); err != nil {
				return err
			}

		case <-time.After(c.callTimeout):
			return status.Error(codes.DeadlineExceeded, "call timeout exceeded")
		}
	}

	return nil
}

func (c *Client) handleCallbackFromServer(ctx context.Context, delivery *amqp.Delivery) error {
	pc, ok := c.pendingCalls.get(delivery.CorrelationId)
	if !ok {
		return fmt.Errorf("error getting pending call by correlation id")
	}

	stStr, ok := delivery.Headers["Amqp-Rpc-Status"].(string)
	if !ok {
		return fmt.Errorf("error read rpc reply message status")
	}

	var code codes.Code
	if err := json.Unmarshal([]byte(stStr), &code); err != nil {
		return fmt.Errorf("error unmarshal rpc message callback status code")
	}
	if code != codes.OK {
		st := spb.Status{Code: int32(code)}

		sdb, ok := delivery.Headers["Amqp-Rpc-Status-Details-Bin"].([]byte)
		if ok {
			if err := proto.Unmarshal(sdb, &st); err != nil {
				return fmt.Errorf("error unmarshal status details")
			}
		}

		msg, ok := delivery.Headers["Amqp-Rpc-Message"].(string)
		if ok {
			st.Message = msg
		}

		pc.st = status.FromProto(&st)
	}

	pc.data = delivery.Body

	c.pendingCalls.set(delivery.CorrelationId, pc)

	pc.done <- struct{}{}

	return nil
}

func (c *Client) Close() error {
	return c.pool.Close()
}

func newCorrID() string {
	return uuid.New().String()
}
