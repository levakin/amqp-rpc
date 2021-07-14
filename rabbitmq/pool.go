package rabbitmq

import (
	"crypto/tls"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	DefaultHeartbeat              = 10 * time.Second
	DefaultLocale                 = "en_US"
	DefaultClientConnectionsCount = 1
	DefaultClientChannelsPoolSize = 20
)

type rabbitConnection struct {
	amqpConn     *amqp.Connection
	closed       chan *amqp.Error
	channelsPool chan *rabbitChannel
}

type rabbitChannel struct {
	channel    *amqp.Channel
	closed     chan *amqp.Error
	rabbitConn *rabbitConnection
}

type Pooler interface {
	GetAMQPChan() (*amqp.Channel, error)
	ReleaseAMQPChan(channel *amqp.Channel)
	io.Closer
}

type Pool struct {
	name    string // Is used to name all rabbitmq connections
	connStr string

	connections    map[*amqp.Connection]*rabbitConnection
	connectionsMut sync.RWMutex

	channels    map[*amqp.Channel]*rabbitChannel
	channelsMut sync.RWMutex

	connPool chan *rabbitConnection
}

type PoolOptions struct {
	ConnectionsCount int
	ChannelsPoolSize int
	TLSCfg           *tls.Config
	Heartbeat        time.Duration
	Locale           string
}

func WithConnectionsCount(n int) func(opts *PoolOptions) {
	return func(opts *PoolOptions) {
		opts.ConnectionsCount = n
	}
}

func WithChannelsPoolSize(n int) func(opts *PoolOptions) {
	return func(opts *PoolOptions) {
		opts.ChannelsPoolSize = n
	}
}

func WithTLSConfig(t *tls.Config) func(opts *PoolOptions) {
	return func(opts *PoolOptions) {
		opts.TLSCfg = t
	}
}

func WithHeartbeat(d time.Duration) func(opts *PoolOptions) {
	return func(opts *PoolOptions) {
		opts.Heartbeat = d
	}
}

func WithLocale(locale string) func(opts *PoolOptions) {
	return func(opts *PoolOptions) {
		opts.Locale = locale
	}
}

func NewPool(connStr string, name string, options ...func(opts *PoolOptions)) (*Pool, error) {
	opts := PoolOptions{
		ConnectionsCount: DefaultClientConnectionsCount,
		ChannelsPoolSize: DefaultClientChannelsPoolSize,
		Heartbeat:        DefaultHeartbeat,
		Locale:           DefaultLocale,
	}
	for _, option := range options {
		option(&opts)
	}

	connectionPool := make(chan *rabbitConnection, opts.ConnectionsCount)

	connections := make(map[*amqp.Connection]*rabbitConnection, opts.ConnectionsCount)
	channels := make(map[*amqp.Channel]*rabbitChannel, opts.ConnectionsCount*opts.ChannelsPoolSize)

	for i := 0; i < opts.ConnectionsCount; i++ {
		cfg := amqp.Config{
			Heartbeat:       opts.Heartbeat,
			TLSClientConfig: opts.TLSCfg,
			Locale:          opts.Locale,
			Properties: amqp.Table{
				"connection_name": name,
			},
		}
		conn, err := amqp.DialConfig(connStr, cfg)
		if err != nil {
			return nil, fmt.Errorf("error dial rabbitmq server: %w", err)
		}

		chanPool := make(chan *rabbitChannel, opts.ChannelsPoolSize)
		rabbitConnectionInst := &rabbitConnection{
			amqpConn:     conn,
			closed:       conn.NotifyClose(make(chan *amqp.Error, 1)),
			channelsPool: chanPool,
		}

		for j := 0; j < opts.ChannelsPoolSize; j++ {
			ch, err := conn.Channel()
			if err != nil {
				return nil, err
			}

			rabbitChannelInst := &rabbitChannel{
				channel:    ch,
				closed:     ch.NotifyClose(make(chan *amqp.Error, 1)),
				rabbitConn: rabbitConnectionInst,
			}

			chanPool <- rabbitChannelInst
			channels[ch] = rabbitChannelInst
		}

		connectionPool <- rabbitConnectionInst
		connections[conn] = rabbitConnectionInst
	}

	return &Pool{
		connStr:     connStr,
		connections: connections,
		channels:    channels,
		connPool:    connectionPool,
		name:        name, // Is used to name all rabbitmq connections
	}, nil
}

func (p *Pool) GetAMQPChan() (*amqp.Channel, error) {
	conn, err := p.getRabbitConnection()
	if err != nil {
		return nil, err
	}
	defer p.releaseRabbitConnection(conn)
	ch := <-conn.channelsPool

	select {
	case <-ch.closed:
		p.channelsMut.Lock()
		delete(p.channels, ch.channel)
		p.channelsMut.Unlock()

		newCh, err := conn.amqpConn.Channel()
		if err != nil {
			return nil, err
		}

		ch = &rabbitChannel{
			channel:    newCh,
			closed:     newCh.NotifyClose(make(chan *amqp.Error, 1)),
			rabbitConn: conn,
		}

		p.channelsMut.Lock()
		p.channels[newCh] = ch
		p.channelsMut.Unlock()
	default:
	}

	return ch.channel, nil
}

func (p *Pool) ReleaseAMQPChan(channel *amqp.Channel) {
	p.channelsMut.RLock()
	ch, ok := p.channels[channel]
	p.channelsMut.RUnlock()

	if !ok {
		return
	}

	p.connectionsMut.RLock()
	conn, ok := p.connections[ch.rabbitConn.amqpConn]
	p.connectionsMut.RUnlock()

	if !ok {
		return
	}
	conn.channelsPool <- ch
}

func (p *Pool) getRabbitConnection() (rabbitConn *rabbitConnection, err error) {
	rabbitConn = <-p.connPool

	defer func(c *rabbitConnection) {
		if err != nil {
			p.connPool <- c
		}
	}(rabbitConn)

	select {
	case <-rabbitConn.closed:

		cfg := amqp.Config{
			Properties: amqp.Table{
				"connection_name": p.name,
			},
		}

		newConn, err := amqp.DialConfig(p.connStr, cfg)
		if err != nil {
			return nil, err
		}

		newRabbitConnection := &rabbitConnection{
			amqpConn:     newConn,
			closed:       newConn.NotifyClose(make(chan *amqp.Error, 1)),
			channelsPool: make(chan *rabbitChannel, cap(rabbitConn.channelsPool)),
		}

		for i := 0; i < cap(newRabbitConnection.channelsPool); i++ {
			newChan, err := newConn.Channel()
			if err != nil {
				return nil, err
			}
			newRabbitChan := &rabbitChannel{
				channel:    newChan,
				closed:     newChan.NotifyClose(make(chan *amqp.Error, 1)),
				rabbitConn: newRabbitConnection,
			}

			newRabbitConnection.channelsPool <- newRabbitChan
			p.channelsMut.Lock()
			p.channels[newChan] = newRabbitChan
			p.channelsMut.Unlock()
		}
		close(rabbitConn.channelsPool)
		p.connectionsMut.Lock()
		delete(p.connections, rabbitConn.amqpConn)

		p.connections[newRabbitConnection.amqpConn] = newRabbitConnection
		p.connectionsMut.Unlock()

		rabbitConn = newRabbitConnection
	default:
	}

	return rabbitConn, nil
}

func (p *Pool) releaseRabbitConnection(conn *rabbitConnection) {
	p.connPool <- conn
}

func (p *Pool) Close() error {
	for k := range p.channels {
		_ = k.Close()
	}

	for c := range p.connections {
		_ = c.Close()
	}
	return nil
}
