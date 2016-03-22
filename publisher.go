package pubsub

import (
	"errors"
	"sync"

	"github.com/garyburd/redigo/redis"
)

// ErrPublishWouldBlock ...
var ErrPublishWouldBlock = errors.New("Publish would block")

// DefaultPublisherPoolSize ...
const DefaultPublisherPoolSize = 16

// DefaultPublisherBufferSize ...
const DefaultPublisherBufferSize = 1 << 20

// Publisher ...
type Publisher interface {
	Publish(channel string, data []byte)
	Shutdown()
}

// PublicationHandler ...
type PublicationHandler interface {
	OnConnect(conn redis.Conn, address string)
	OnPublishError(err error, channel string, data []byte)
}

type message struct {
	channel string
	data    []byte
}

type redisPublisher struct {
	pool     *redis.Pool
	handler  PublicationHandler
	messages chan *message
	wg       sync.WaitGroup
}

// NewRedisPublisher ...
func NewRedisPublisher(address string, handler PublicationHandler, poolSize, bufferSize int) Publisher {
	if poolSize == 0 {
		poolSize = DefaultPublisherPoolSize
	}
	if bufferSize == 0 {
		bufferSize = DefaultPublisherBufferSize
	}
	p := &redisPublisher{
		pool: &redis.Pool{
			MaxIdle: poolSize,
			Dial: func() (redis.Conn, error) {
				conn, err := redis.Dial("tcp", address)
				if err != nil {
					return nil, err
				}
				handler.OnConnect(conn, address)
				return conn, err
			},
		},
		messages: make(chan *message, bufferSize),
		handler:  handler,
	}
	// start the workers
	for i := 0; i < poolSize; i++ {
		p.wg.Add(1)
		go p.publishLoop()
	}
	return p
}

func (p *redisPublisher) publishLoop() {
	for m := range p.messages {
		func() {
			conn := p.pool.Get()
			defer conn.Close()
			if _, err := conn.Do("PUBLISH", m.channel, m.data); err != nil {
				p.handler.OnPublishError(err, m.channel, m.data)
			}
		}()
	}
	p.wg.Done()
}

// Publish ...
func (p *redisPublisher) Publish(channel string, data []byte) {
	select {
	case p.messages <- &message{channel: channel, data: data}:
	default:
		p.handler.OnPublishError(ErrPublishWouldBlock, channel, data)
	}
}

// Shutdown ...
func (p *redisPublisher) Shutdown() {
	close(p.messages)
	p.pool.Close()
	p.wg.Wait()
}
