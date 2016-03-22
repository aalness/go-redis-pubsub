package pubsub

import (
	"github.com/garyburd/redigo/redis"
)

// DefaultPublisherPoolSize ...
const DefaultPublisherPoolSize = 16

// Publisher ...
type Publisher interface {
	Publish(channel string, data []byte) error
	Shutdown()
}

type redisPublisher struct {
	pool *redis.Pool
}

// NewRedisPublisher ...
func NewRedisPublisher(poolSize int, address string) Publisher {
	if poolSize == 0 {
		poolSize = DefaultPublisherPoolSize
	}
	return &redisPublisher{
		pool: &redis.Pool{
			MaxIdle: poolSize,
			Dial: func() (redis.Conn, error) {
				conn, err := redis.Dial("tcp", address)
				if err != nil {
					return nil, err
				}
				return conn, err
			},
		},
	}
}

// Publish ...
func (p *redisPublisher) Publish(channel string, data []byte) error {
	conn := p.pool.Get()
	defer conn.Close()
	_, err := conn.Do("PUBLISH", channel, data)
	return err
}

// Shutdown ...
func (p *redisPublisher) Shutdown() {
	p.pool.Close()
}
