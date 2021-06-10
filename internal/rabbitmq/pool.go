package rabbitmq

import (
	"crypto/tls"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

type Pool interface {
	GetAMQPChan() (*amqp.Channel, error)
	ReleaseAMQPChan(channel *amqp.Channel)
	io.Closer
}

type pool struct {
	name    string // Is used to name all rabbitmq connections
	connStr string

	connections    map[*amqp.Connection]*rabbitConnection
	connectionsMut sync.RWMutex

	channels    map[*amqp.Channel]*rabbitChannel
	channelsMut sync.RWMutex

	connPool chan *rabbitConnection
}

func (p *pool) GetAMQPChan() (*amqp.Channel, error) {
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

func (p *pool) ReleaseAMQPChan(channel *amqp.Channel) {
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

func (p *pool) getRabbitConnection() (rabbitConn *rabbitConnection, err error) {
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

func (p *pool) releaseRabbitConnection(conn *rabbitConnection) {
	p.connPool <- conn
}

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

const (
	defaultHeartbeat = 10 * time.Second
	defaultLocale    = "en_US"
)

func NewPool(connStr string, connPoolSize, chanPoolSize int, name string, tlsCfg *tls.Config) (Pool, error) {
	connectionPool := make(chan *rabbitConnection, connPoolSize)

	connections := make(map[*amqp.Connection]*rabbitConnection, connPoolSize)
	channels := make(map[*amqp.Channel]*rabbitChannel, connPoolSize*chanPoolSize)

	for i := 0; i < connPoolSize; i++ {
		cfg := amqp.Config{
			Heartbeat:       defaultHeartbeat,
			TLSClientConfig: tlsCfg,
			Locale:          defaultLocale,
			Properties: amqp.Table{
				"connection_name": name,
			},
		}
		conn, err := amqp.DialConfig(connStr, cfg)
		if err != nil {
			return nil, fmt.Errorf("error dial rabbitmq server. OError: %v", err)
		}

		chanPool := make(chan *rabbitChannel, chanPoolSize)
		rabbitConnectionInst := &rabbitConnection{
			amqpConn:     conn,
			closed:       conn.NotifyClose(make(chan *amqp.Error, 1)),
			channelsPool: chanPool,
		}

		for j := 0; j < chanPoolSize; j++ {
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

	return &pool{
		connStr:     connStr,
		connections: connections,
		channels:    channels,
		connPool:    connectionPool,
		name:        name, // Is used to name all rabbitmq connections
	}, nil
}

func (p *pool) Close() error {
	for k := range p.channels {
		_ = k.Close()
	}

	for c := range p.connections {
		_ = c.Close()
	}
	return nil
}
