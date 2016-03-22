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
	Subscribe(channel string) <-chan error
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
	counts     map[string]int
	pending    map[string][]chan error
	timers     map[string]context.CancelFunc
}

// subscribe subscribers to a new channel and/or increments its channel subscription counter.
func (c *redisSubscriberConn) subscribe(channel string) <-chan error {
	errChan := make(chan error, 1)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	count, ok := 0, false
	if count, ok = c.counts[channel]; ok {
		count++
	} else {
		count = 1
	}
	// update subscriber count
	c.counts[channel] = count
	// add to the pending list
	if errChans, ok := c.pending[channel]; ok {
		if count == 1 {
			panic("pending list not empty")
		}
		errChans = append(errChans, errChan)
	} else {
		// first subscriber
		if count == 1 {
			if _, ok := c.timers[channel]; ok {
				// cancel existing unsubscribe timer
				c.setUnsubscribeTimerLocked(channel, 0)
				// already subscribed
				errChan <- nil
			} else {
				// add to pending list
				c.pending[channel] = []chan error{errChan}
				// send the SUBSCRIBE+FLUSH commands
				if err := c.conn.Subscribe(channel); err != nil {
					errChan <- err
					delete(c.counts, channel)
					delete(c.pending, channel)
				}
			}
		} else {
			// already subscribed
			errChan <- nil
		}
	}
	return errChan
}

// unsubscribe decrements its connection counter; on 0 count an unsubscribe timer is started.
func (c *redisSubscriberConn) unsubscribe(channel string) (int, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	count, ok := 0, false
	if count, ok = c.counts[channel]; ok {
		if count--; count == 0 {
			delete(c.counts, channel)
			// start the unsubscribe timer for this channel
			timeout := c.subscriber.handler.GetUnsubscribeTimeout()
			c.setUnsubscribeTimerLocked(channel, timeout)
		} else {
			c.counts[channel] = count
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
				if _, ok := c.counts[channel]; !ok {
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
					for channel := range c.counts {
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
				func() {
					// send signal for all pending subscriptions to this channel
					c.mutex.Lock()
					defer c.mutex.Unlock()
					if chans, ok := c.pending[msg.Channel]; ok {
						for _, errChan := range chans {
							errChan <- nil
						}
					}
					delete(c.pending, msg.Channel)
				}()
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
	// clear channel subscription counts
	c.counts = make(map[string]int)
	// cancel all timers
	for _, cancel := range c.timers {
		cancel()
	}
	c.timers = make(map[string]context.CancelFunc)
	// send error signal for all pending subscriptions
	for _, chans := range c.pending {
		for _, errChan := range chans {
			errChan <- io.EOF
		}
	}
	c.pending = make(map[string][]chan error)
}

// redisSubscriber ...
type redisSubscriber struct {
	address       string
	handler       SubscriptionHandler
	slotMutexes   []sync.RWMutex
	slots         []*redisSubscriberConn
	shutdownMutex sync.RWMutex
	shutdown      bool
}

// NewRedisSubscriber ...
func NewRedisSubscriber(poolSize int, address string, handler SubscriptionHandler) Subscriber {
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
	return subscriber
}

func (s *redisSubscriber) reconnectSlot(slot int) {
	// connect. todo: let handler specify backoff parameters
	expBackoff := backoff.NewExponentialBackOff()
	// don't quit trying
	expBackoff.MaxElapsedTime = 0

	var conn redis.Conn
	err := backoff.RetryNotify(func() error {
		// quit reconnecting if shutting down
		if s.isShutdown() {
			return nil
		}
		var err error
		conn, err = redis.Dial("tcp", s.address)
		return err
	}, expBackoff, s.handler.OnConnectError)

	// shouldn't be possible
	if err != nil {
		panic(err)
	}

	// prevent respawning a receive loop
	s.shutdownMutex.RLock()
	defer s.shutdownMutex.RUnlock()
	if s.shutdown {
		if conn != nil {
			conn.Close()
		}
		return
	}

	// create the connection
	connection := &redisSubscriberConn{
		slot:       slot,
		subscriber: s,
		conn:       &redis.PubSubConn{Conn: conn},
		counts:     make(map[string]int),
		pending:    make(map[string][]chan error),
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
func (s *redisSubscriber) Subscribe(channel string) <-chan error {
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
	s.shutdownMutex.RLock()
	defer s.shutdownMutex.RUnlock()
	return s.shutdown
}

// Shutdown ...
func (s *redisSubscriber) Shutdown() {
	s.shutdownMutex.Lock()
	defer s.shutdownMutex.Unlock()
	s.shutdown = true
	for _, conn := range s.slots {
		conn.close()
	}
}
