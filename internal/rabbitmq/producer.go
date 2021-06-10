package rabbitmq

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
	"github.com/streadway/amqp"
)

type Producer struct {
	name string
	pool Pool
}

func NewProducer(name string, pool Pool) (*Producer, error) {
	return &Producer{name: name, pool: pool}, nil
}

func (p *Producer) Close() error {
	return p.pool.Close()
}

func (p *Producer) Publish(ctx context.Context, pub amqp.Publishing, exchange, key string) error {
	log.Debug().Msgf("%q: publishing message to rabbitmq...", p.name)

	ch, err := p.pool.GetAMQPChan()
	if err != nil {
		return err
	}
	defer p.pool.ReleaseAMQPChan(ch)

	errCh := make(chan error, 1)
	doneCh := make(chan struct{}, 1)

	go func() {
		err = ch.Publish(exchange, key, false, false, pub)
		if err != nil {
			errCh <- fmt.Errorf("%q: error publishing message %v: %w", p.name, pub, err)
		}
		doneCh <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		log.Error().Msgf("%q: context exceeded while sending the result to the rabbitmq", p.name)
	case err := <-errCh:
		log.Error().Msgf("%q: error occur while sending the result to the rabbitmq. OError: %v", p.name, err)
	case <-doneCh:
	}

	log.Debug().Msgf("%q: rabbitmq message sent", p.name)
	return nil
}
