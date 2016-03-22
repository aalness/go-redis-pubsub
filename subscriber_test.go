package pubsub

import (
	"sync"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

type testSubHandler struct {
	t                  *testing.T
	mutex              sync.Mutex
	unsubscribeErrors  int
	receiveErrors      int
	disconnectedErrors int
	subscribeCount     int
	unsubscribeCount   int
	messages           map[string]map[string]struct{}
	messageChan        chan struct{}
	unsubscribeChan    chan struct{}
}

func newTestSubHandler(t *testing.T) *testSubHandler {
	return &testSubHandler{
		t:               t,
		messages:        make(map[string]map[string]struct{}),
		messageChan:     make(chan struct{}, 10000),
		unsubscribeChan: make(chan struct{}, 10000),
	}
}

func (h *testSubHandler) OnConnectError(err error, nextTime time.Duration) {
	h.t.Fatal(err)
}

func (h *testSubHandler) OnSubscribe(channel string, count int) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.subscribeCount++
}

func (h *testSubHandler) OnUnsubscribe(channel string, count int) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.unsubscribeCount++
	h.unsubscribeChan <- struct{}{}
}

func (h *testSubHandler) OnMessage(channel string, data []byte) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	message, _ := redis.String(data, nil)
	messages, ok := h.messages[channel]
	if !ok {
		messages = make(map[string]struct{})
		h.messages[channel] = messages
	}
	messages[message] = struct{}{}
	h.messageChan <- struct{}{}
}

func (h *testSubHandler) OnUnsubscribeError(channel string, err error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.unsubscribeErrors++
}

func (h *testSubHandler) OnReceiveError(err error) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.receiveErrors++
}

func (h *testSubHandler) OnDisconnected(err error, channels []string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.disconnectedErrors++
}

func (h *testSubHandler) GetUnsubscribeTimeout() time.Duration {
	return 1 * time.Millisecond
}

func (h *testSubHandler) waitForMessages(count int) {
	seen := 0
	for {
		select {
		case <-h.messageChan:
			if seen++; seen == count {
				return
			}
		case <-time.After(30 * time.Second):
			h.t.Fatal("Timed out waiting for messages")
		}
	}
}

func (h *testSubHandler) waitForUnsubscribes(count int) {
	seen := 0
	for {
		select {
		case <-h.unsubscribeChan:
			if seen++; seen == count {
				return
			}
		case <-time.After(30 * time.Second):
			h.t.Fatal("Timed out waiting for unsubscribes")
		}
	}
}

func TestSubscriberBasic(t *testing.T) {
	h := newTestSubHandler(t)
	s := NewRedisSubscriber("localhost:6379", h, 0)

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

	if count, err := s.Unsubscribe("foo"); err != nil {
		t.Fatal(err)
	} else {
		// expect one more subscriber left
		if count != 1 {
			t.Fatalf("Exepected 1 subscriber, got %d", count)
		}
	}
	if count, err := s.Unsubscribe("foo"); err != nil {
		t.Fatal(err)
	} else {
		// no more expected
		if count != 0 {
			t.Fatalf("Exepected 0 subscribers, got %d", count)
		}
	}
	// shouldn't be subscribed anymore
	if _, err := s.Unsubscribe("foo"); err != ErrNotSubscribed {
		t.Fatalf("Expected ErrNotSubscribed, got: %v", err)
	}

	h.waitForUnsubscribes(1)

	// subscribe to foo again
	if err := <-s.Subscribe("foo"); err != nil {
		t.Fatal(err)
	}
	// expect 2 subscribes now
	if h.subscribeCount != 2 {
		t.Fatalf("Exepected 2 subscriptions, got %d", h.subscribeCount)
	}

	// subscribe to bar
	if err := <-s.Subscribe("bar"); err != nil {
		t.Fatal(err)
	}
	// expect 3 subscribes now
	if h.subscribeCount != 3 {
		t.Fatalf("Exepected 3 subscriptions, got %d", h.subscribeCount)
	}

	// just the one unsubscribe expected
	if h.unsubscribeCount != 1 {
		t.Fatalf("Exepected 1 unsubscription, got %d", h.unsubscribeCount)
	}

	s.Shutdown()
}
