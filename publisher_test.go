package pubsub

import (
	"strconv"
	"sync"
	"testing"

	"github.com/garyburd/redigo/redis"
)

type testPubHandler struct {
	t           *testing.T
	mutex       sync.Mutex
	connections int
}

func (h *testPubHandler) OnPublishConnect(conn redis.Conn, address string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.connections++
}

func (h *testPubHandler) OnPublishError(err error, channel string, data []byte) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.t.Fatal(err)
}

func TestPublisherBasic(t *testing.T) {
	sh := newTestSubHandler(t)
	s := NewRedisSubscriber("localhost:6379", sh, 0)

	ph := &testPubHandler{t: t}
	p := NewRedisPublisher("localhost:6379", ph, 0, 0)

	count := 100
	channels := []string{"foo", "bar", "hi"}

	// subscribe to all channels
	for _, channel := range channels {
		if err := <-s.Subscribe(channel); err != nil {
			t.Fatal(err)
		}
	}

	// publish 100 messages to each channel concurrently
	var wg sync.WaitGroup
	wg.Add(count * len(channels))
	for _, channel := range channels {
		for i := 0; i < count; i++ {
			go func(channel string, i int) {
				p.Publish(channel, []byte(strconv.Itoa(i)))
				wg.Done()
			}(channel, i)
		}
	}
	wg.Wait()

	// wait for all messages
	sh.waitForMessages(count * len(channels))

	// check the messages
	for _, channel := range channels {
		messages := sh.messages[channel]
		for i := 0; i < count; i++ {
			if _, ok := messages[strconv.Itoa(i)]; !ok {
				t.Fatalf("%d not found from channel %s", i, channel)
			}
		}
	}

	if ph.connections != DefaultPublisherPoolSize {
		t.Fatalf("Expected %d connections, got: %d", DefaultPublisherPoolSize, ph.connections)
	}

	s.Shutdown()
	p.Shutdown()
}
