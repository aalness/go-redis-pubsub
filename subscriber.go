package pubsub

import (
	"errors"
	"hash/fnv"
	"io"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/garyburd/redigo/redis"
	"golang.org/x/net/context"
)

// ErrNotSubscribed ...
var ErrNotSubscribed = errors.New("Not subscribed")

// DefaultSubscriberPoolSize ...
const DefaultSubscriberPoolSize = 16

// Subscriber ...
type Subscriber interface {
	// Subscribe ...
	Subscribe(channel string) (count int, err error)
	// Unsubscribe ...
	Unsubscribe(channel string) (count int, err error)
	// Shutdown ...
	Shutdown()
}

// SubscriptionHandler ...
type SubscriptionHandler interface {
	// OnConnectError ...
	OnConnectError(err error, nextTime time.Duration)
	// OnSubscribe ...
	OnSubscribe(channel string, count int)
	// OnUnsubscribe ...
	OnUnsubscribe(channel string, count int)
	// OnMessage ...
	OnMessage(channel string, data []byte)
	// OnUnsubscribeError ...
	OnUnsubscribeError(channel string, err error)
	// OnReceiveError ...
	OnReceiveError(err error)
	// OnDisconnected ...
	OnDisconnected(err error, channels []string)
	// GetUnsubscribeTimeout ...
	GetUnsubscribeTimeout() time.Duration
}

// redisSubscriberConn implements a subscriber connection with redis.
type redisSubscriberConn struct {
	slot       int
	subscriber *redisSubscriber
	mutex      sync.Mutex
	conn       *redis.PubSubConn
	channels   map[string]int
	timers     map[string]context.CancelFunc
}

// subscribe subscribers to a new channel and/or increments its channel subscription counter.
func (c *redisSubscriberConn) subscribe(channel string) (int, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	count, ok := 0, false
	if count, ok = c.channels[channel]; ok {
		count++
		c.channels[channel] = count
	} else {
		// send the SUBSCRIBE+FLUSH commands
		if err := c.conn.Subscribe(channel); err != nil {
			return 0, err
		}
		count = 1
		c.channels[channel] = count
		// cancel any existing unsubscribe timer for this channel
		c.setUnsubscribeTimerLocked(channel, 0)
	}
	return count, nil
}

// unsubscribe decrements its connection counter; on 0 count an unsubscribe timer is started.
func (c *redisSubscriberConn) unsubscribe(channel string) (int, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	count, ok := 0, false
	if count, ok = c.channels[channel]; ok {
		if count--; count == 0 {
			delete(c.channels, channel)
			// start the unsubscribe timer for this channel
			timeout := c.subscriber.handler.GetUnsubscribeTimeout()
			c.setUnsubscribeTimerLocked(channel, timeout)
		} else {
			c.channels[channel] = count
		}
	} else {
		return 0, ErrNotSubscribed
	}
	return count, nil
}

func (c *redisSubscriberConn) setUnsubscribeTimerLocked(channel string, timeout time.Duration) {
	// cancel any existing timer
	if cancel, ok := c.timers[channel]; ok {
		delete(c.timers, channel)
		cancel()
	}
	if timeout == 0 {
		return
	}
	// start a new timer
	timer := time.NewTimer(timeout)
	ctx, cancel := context.WithCancel(context.Background())
	c.timers[channel] = cancel
	go func() {
		defer cancel()
		select {
		case <-timer.C:
			err := func() error {
				c.mutex.Lock()
				defer c.mutex.Unlock()
				if _, ok := c.channels[channel]; !ok {
					// remove the cancel function for this channel
					delete(c.timers, channel)
					// send the UNSUBSCRIBE+FLUSH commands
					return c.conn.Unsubscribe(channel)
				}
				return nil
			}()
			if err != nil {
				// notify the handler
				c.subscriber.handler.OnUnsubscribeError(channel, err)
			}
		case <-ctx.Done():
		}
	}()
}

func (c *redisSubscriberConn) isDisconnectError(err error) bool {
	switch err {
	case io.EOF:
		fallthrough
	case io.ErrUnexpectedEOF:
		fallthrough
	case io.ErrClosedPipe:
		return true
	default:
		return false
	}
}

func (c *redisSubscriberConn) receiveLoop() {
	for {
		switch msg := c.conn.Receive().(type) {
		case error:
			if c.isDisconnectError(msg) {
				var channels []string
				func() {
					c.mutex.Lock()
					defer c.mutex.Unlock()
					for channel := range c.channels {
						channels = append(channels, channel)
					}
					// close the connection
					c.closeLocked()
				}()
				// notify the subscription handler of channels we're no longer tracking
				c.subscriber.handler.OnDisconnected(msg, channels)
				// reconnect
				c.subscriber.reconnectSlot(c.slot)
				return
			}
			c.subscriber.handler.OnReceiveError(msg)
		case redis.Message:
			// notify handler of new message
			c.subscriber.handler.OnMessage(msg.Channel, msg.Data)
		case redis.Subscription:
			// notify handler of new subscription event
			if msg.Kind == "subscribe" {
				c.subscriber.handler.OnSubscribe(msg.Channel, msg.Count)
			} else if msg.Kind == "unsubscribe" {
				c.subscriber.handler.OnUnsubscribe(msg.Channel, msg.Count)
			}
		}
	}
}

func (c *redisSubscriberConn) close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.closeLocked()
}

func (c *redisSubscriberConn) closeLocked() {
	// close the connection
	c.conn.Close()
	// clear channels
	c.channels = make(map[string]int)
	// cancel all timers
	for _, cancel := range c.timers {
		cancel()
	}
	c.timers = make(map[string]context.CancelFunc)
}

// redisSubscriber ...
type redisSubscriber struct {
	address       string
	handler       SubscriptionHandler
	slotMutexes   []sync.RWMutex
	slots         []*redisSubscriberConn
	shutdownMutex sync.Mutex
	shutdown      bool
}

// NewRedisSubscriber ...
func NewRedisSubscriber(poolSize int, address string, handler SubscriptionHandler) (
	Subscriber, error) {
	if poolSize == 0 {
		poolSize = DefaultSubscriberPoolSize
	}
	// create the subscriber
	subscriber := &redisSubscriber{
		address:     address,
		handler:     handler,
		slotMutexes: make([]sync.RWMutex, poolSize),
		slots:       make([]*redisSubscriberConn, poolSize),
	}
	// connect
	for slot := 0; slot < poolSize; slot++ {
		subscriber.reconnectSlot(slot)
	}
	return subscriber, nil
}

func (s *redisSubscriber) reconnectSlot(slot int) {
	// don't reconnect if shutting down
	if s.isShutdown() {
		return
	}

	// connect. todo: let handler specify backoff parameters
	expBackoff := backoff.NewExponentialBackOff()
	// don't quit trying
	expBackoff.MaxElapsedTime = 0

	var conn redis.Conn
	err := backoff.RetryNotify(func() error {
		var err error
		conn, err = redis.Dial("tcp", s.address)
		return err
	}, expBackoff, s.handler.OnConnectError)

	// shouldn't be possible
	if err != nil {
		panic(err)
	}

	// create the connection
	connection := &redisSubscriberConn{
		slot:       slot,
		subscriber: s,
		conn:       &redis.PubSubConn{Conn: conn},
		channels:   make(map[string]int),
		timers:     make(map[string]context.CancelFunc),
	}
	func() {
		// save it to its slot
		s.slotMutexes[slot].Lock()
		defer s.slotMutexes[slot].Unlock()
		s.slots[slot] = connection
	}()

	// start the receive loop
	go connection.receiveLoop()
}

func (s *redisSubscriber) getSlot(channel string) int {
	// attempt to evenly spread channels over available connections.
	// this mitigates the impact of a single disconnection and spreads load.
	h := fnv.New32a()
	h.Write([]byte(channel))
	return int(h.Sum32() % uint32(len(s.slots)))
}

// Subscribe ...
func (s *redisSubscriber) Subscribe(channel string) (int, error) {
	slot := s.getSlot(channel)
	s.slotMutexes[slot].RLock()
	defer s.slotMutexes[slot].RUnlock()
	return s.slots[slot].subscribe(channel)
}

// Unsubscribe ...
func (s *redisSubscriber) Unsubscribe(channel string) (int, error) {
	slot := s.getSlot(channel)
	s.slotMutexes[slot].RLock()
	defer s.slotMutexes[slot].RUnlock()
	return s.slots[slot].unsubscribe(channel)
}

func (s *redisSubscriber) isShutdown() bool {
	s.shutdownMutex.Lock()
	defer s.shutdownMutex.Lock()
	return s.shutdown
}

// Shutdown ...
func (s *redisSubscriber) Shutdown() {
	s.shutdownMutex.Lock()
	defer s.shutdownMutex.Lock()
	s.shutdown = true
	for _, conn := range s.slots {
		conn.close()
	}
}