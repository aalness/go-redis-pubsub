package pubsub

import (
	"sync"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

type TestHandler struct {
	t                  *testing.T
	mutex              sync.Mutex
	unsubscribeErrors  int
	receiveErrors      int
	disconnectedErrors int
	subscribeCount     int
	unsubscribeCount   int
	messages           map[string]string
}

func (h *TestHandler) OnConnectError(err error, nextTime time.Duration) {
	h.t.Fatal(err)
}

func (h *TestHandler) OnSubscribe(channel string, count int) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.subscribeCount++
}

func (h *TestHandler) OnUnsubscribe(channel string, count int) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.unsubscribeCount++
}

func (h *TestHandler) OnMessage(channel string, data []byte) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	message, _ := redis.String(data, nil)
	h.messages[channel] = message
}

func (h *TestHandler) OnUnsubscribeError(channel string, err error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.unsubscribeErrors++
}

func (h *TestHandler) OnReceiveError(err error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.receiveErrors++
}

func (h *TestHandler) OnDisconnected(err error, channels []string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.disconnectedErrors++
}

func (h *TestHandler) GetUnsubscribeTimeout() time.Duration {
	return 1 * time.Millisecond
}

func TestSubscriberBasic(t *testing.T) {
	h := &TestHandler{
		t:        t,
		messages: make(map[string]string),
	}
	s := NewRedisSubscriber(0, "localhost:6379", h)

	if err := <-s.Subscribe("foo"); err != nil {
		t.Fatal(err)
	}
	if err := <-s.Subscribe("foo"); err != nil {
		t.Fatal(err)
	}
	// should only subscribe once
	if h.subscribeCount != 1 {
		t.Fatalf("Exepected 1 subscription, got %d", h.subscribeCount)
	}

	s.Shutdown()
}
