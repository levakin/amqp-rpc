package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"
)

const reconnectDelay = time.Second * 5

type Consumer struct {
	pool    Pool
	queue   string
	workers int
	name    string
}

func NewConsumer(pool Pool, queue string, workers int, name string) (*Consumer, error) {
	ch, err := pool.GetAMQPChan()
	if err != nil {
		return nil, err
	}
	defer pool.ReleaseAMQPChan(ch)

	if _, err = ch.QueueDeclare(
		queue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return nil, err
	}

	return &Consumer{
		pool:    pool,
		workers: workers,
		queue:   queue,
		name:    name,
	}, nil
}

type DeliveryHandlerFunc func(ctx context.Context, delivery *amqp.Delivery) error

func (d DeliveryHandlerFunc) Handle(ctx context.Context, delivery *amqp.Delivery) error {
	return d(ctx, delivery)
}

type DeliveryHandler interface {
	Handle(ctx context.Context, delivery *amqp.Delivery) error
}

func (c *Consumer) Serve(ctx context.Context, handler DeliveryHandler) error {
	log.Info().Msgf("%q:starting rabbitMQ consumer with %d workers...", c.name, c.workers)

	wg := new(sync.WaitGroup)
	wg.Add(c.workers)

	for i := 0; i < c.workers; i++ {
		go func(workerId int) {
			defer wg.Done()
			defer log.Info().Msgf("consumer %q: worker %d: stopped", c.name, workerId)

			for {
				select {
				case <-ctx.Done():
					return
				default:
					if err := c.startWorker(ctx, workerId, handler); err != nil {
						log.Error().Stack().Err(err).Msgf("consumer %q: worker %d: got error", c.name, workerId)
					}
					log.Info().Msgf(`%q: restarting worker:"%d"...`, c.name, workerId)

					select {
					case <-ctx.Done():
						return
					case <-time.After(reconnectDelay):
					}
				}
			}
		}(i)
	}
	// wait for shutdown signal
	<-ctx.Done()
	log.Info().Msgf("%q: got signal to shutdown. waiting for all workers to terminate...", c.name)

	// wait for all workers to terminate
	wg.Wait()

	log.Info().Msgf("%q: all workers have been stopped", c.name)

	return nil
}

func (c *Consumer) startWorker(ctx context.Context, workerId int, handler DeliveryHandler) (err error) {
	ch, err := c.pool.GetAMQPChan()
	if err != nil {
		return fmt.Errorf("worker %q:%d: error get channel for rabbitmq provider. OError: %v", c.name, workerId, err)
	}
	defer c.pool.ReleaseAMQPChan(ch)

	if err := ch.Qos(1, 0, false); err != nil {
		return fmt.Errorf("worker %q:%d: error set qos for rabbitmq provider. OError: %v", c.name, workerId, err)
	}

	deliveryChan, err := ch.Consume(c.queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("worker %q:%d: error get delivery channel for amqp channel. OError: %v", c.name, workerId, err)
	}

	log.Info().Msgf(`worker %q:%d" started`, c.name, workerId)
	for {
		select {
		case message, ok := <-deliveryChan:
			if !ok {
				return fmt.Errorf("worker %q:%d: unable to read from channel. worker is dead", c.name, workerId)
			}

			func(m amqp.Delivery) {
				defer func() {
					if err := recover(); err != nil {
						log.Error().Stack().Err(err.(error)).Send()
					}
				}()

				log.Debug().Msgf("worker %q:%d: new amqp message received", c.name, workerId)
				err = handler.Handle(ctx, &message)
				if err != nil {
					log.Error().Msgf("worker %q:%d: error process delivery message: %v", c.name, workerId, err)

					time.Sleep(time.Second * 15) // Sleep before Nack the message. To prevent quick cycle of reading and Nacking the message

					err = message.Nack(false, true)
					log.Debug().Msgf("worker %q:%d: message not acknowledged", c.name, workerId)

					if err != nil {
						err = fmt.Errorf("worker %q:%d: error nack delivery message: %v", c.name, workerId, err)
					}
					return
				}

				err = message.Ack(false)
				log.Debug().Msgf("worker %q:%d: message acknowledged", c.name, workerId)
				if err != nil {
					err = fmt.Errorf("worker %q:%d: error ack delivery message: %v", c.name, workerId, err)
				}
			}(message)
		case <-ctx.Done():
			return
		}
	}
}
