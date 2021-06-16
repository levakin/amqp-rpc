package rpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
	"google.golang.org/protobuf/proto"

	"github.com/levakin/amqp-rpc/codes"
	"github.com/levakin/amqp-rpc/internal/rabbitmq"
	"github.com/levakin/amqp-rpc/status"
)

type rabbitMqServer struct {
	producerPool, consumerPool rabbitmq.Pool
	consumer                   *rabbitmq.Consumer
	producer                   *rabbitmq.Producer
	services                   map[string]*ServiceInfo

	started bool
	mu      sync.Mutex
}

func NewRabbitMQServer(connStr, queue string, connectionsCount, channelsPoolSize int, workers int, tlsCfg *tls.Config) (*rabbitMqServer, error) {
	consumerPool, err := rabbitmq.NewPool(connStr, connectionsCount, channelsPoolSize, "server_consumer_pool", tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("error connecting to rabbitmq server. Conn string: %s", connStr)
	}

	consumer, err := rabbitmq.NewConsumer(consumerPool, queue, workers, "server_consumer")
	if err != nil {
		return nil, err
	}

	producerPool, err := rabbitmq.NewPool(connStr, connectionsCount, channelsPoolSize, "server_producer_pool", tlsCfg)
	if err != nil {
		return nil, fmt.Errorf("error connecting to rabbitmq server. Conn string: %s", connStr)
	}

	producer, err := rabbitmq.NewProducer("callback_sender", producerPool)
	if err != nil {
		return nil, err
	}

	return &rabbitMqServer{
		producerPool: producerPool,
		consumerPool: consumerPool,
		consumer:     consumer,
		producer:     producer,
		services:     make(map[string]*ServiceInfo),
	}, nil
}

func (s *rabbitMqServer) Serve(ctx context.Context) error {
	s.started = true
	return s.consumer.Serve(ctx, rabbitmq.DeliveryHandlerFunc(s.handleDelivery))
}

func (s *rabbitMqServer) Close() error {
	s.started = false
	if err := s.producerPool.Close(); err != nil {
		return err
	}
	return s.consumerPool.Close()
}

// should be sync to control the number of threads running as the same time
func (s *rabbitMqServer) handleDelivery(ctx context.Context, delivery *amqp.Delivery) error {
	fullMethod := delivery.Headers["fullMethod"].(string)
	splittedMethod := strings.Split(fullMethod, "/")

	reqService := splittedMethod[1]
	reqMethod := splittedMethod[2]

	serviceInfo, ok := s.services[reqService]
	if !ok {
		return fmt.Errorf("no such service: " + reqService)
	}

	methodDesc, ok := serviceInfo.Methods[reqMethod]
	if !ok {
		return fmt.Errorf("no such methodDesc: " + reqMethod)
	}

	dec := func(i interface{}) error {
		if err := proto.Unmarshal(delivery.Body, i.(proto.Message)); err != nil {
			return err
		}
		return nil
	}

	pub := amqp.Publishing{
		// Headers: amqp.Table{
		//	"ttl": 100, // When reached 0 message should be deleted from the queue forever
		// },
		CorrelationId: delivery.CorrelationId,
		ContentType:   ProtobufContentType,
		DeliveryMode:  amqp.Persistent,
	}

	reply, appErr := methodDesc.Handler(serviceInfo.ServiceImpl, ctx, dec, nil)
	if appErr != nil {
		appStatus, ok := status.FromError(appErr)
		if !ok {
			// Convert appErr if it is not a amqp rpc status error.
			appErr = status.Error(codes.Unknown, appErr.Error())
			appStatus, _ = status.FromError(appErr)
		}

		err := s.setStatus(&pub, appStatus)
		if err != nil {
			return err
		}
		return s.sendResponse(ctx, pub, "", delivery.ReplyTo)

	} else {

		if delivery.ReplyTo != "" {

			replyData, err := proto.Marshal(reply.(proto.Message))
			if err != nil {
				return err
			}

			pub.Body = replyData
			pub.Headers = map[string]interface{}{
				"Amqp-Rpc-Status": fmt.Sprintf("%d", codes.OK),
			}

			return s.sendResponse(ctx, pub, "", delivery.ReplyTo)
		}
	}

	return nil
}

// setStatus sets status to publishing
func (s *rabbitMqServer) setStatus(pub *amqp.Publishing, st *status.Status) error {
	// Write status code
	h := map[string]interface{}{
		"Amqp-Rpc-Status": fmt.Sprintf("%d", st.Code()),
	}

	// Write status message if exists
	if m := st.Message(); m != "" {
		h["Amqp-Rpc-Message"] = m
	}

	// Write status details if exist
	if p := st.Proto(); p != nil && len(p.Details) > 0 {
		stBytes, err := proto.Marshal(p)
		if err != nil {
			return err
		}
		h["Amqp-Rpc-Status-Details-Bin"] = stBytes
	}
	pub.Headers = h

	return nil
}

func (s *rabbitMqServer) RegisterProtoService(serviceDesc *ServiceDesc, impl interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Infof("amqp-rpc: registring proto service: %q", serviceDesc.ServiceName)
	if s.started {
		log.Fatalf("amqp-rpc: register proto service %q called after server started", serviceDesc.ServiceName)
	}

	if _, ok := s.services[serviceDesc.ServiceName]; ok {
		log.Fatalf("amqp-rpc: found duplicate service registration for %q", serviceDesc.ServiceName)
	}

	methods := make(map[string]*MethodDesc)

	for i := range serviceDesc.Methods {
		methodDesc := &serviceDesc.Methods[i]
		log.Debugf("registring method: %q for service %q", methodDesc.MethodName, serviceDesc.ServiceName)
		methods[methodDesc.MethodName] = methodDesc
	}

	serviceInfo := &ServiceInfo{
		ServiceImpl: impl,
		Methods:     methods,
		Metadata:    serviceDesc.Metadata,
	}

	s.services[serviceDesc.ServiceName] = serviceInfo
	log.Debugf("%d methods registered for service %q", len(methods), serviceDesc.ServiceName)
}

func (s *rabbitMqServer) sendResponse(ctx context.Context, pub amqp.Publishing, exchange, routingKey string) error {
	return s.producer.Publish(ctx, pub, exchange, routingKey)
}
