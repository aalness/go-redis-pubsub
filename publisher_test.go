package pubsub

import (
	"strconv"
	"sync"
	"testing"
)

func TestPublisherBasic(t *testing.T) {
	h := newTestHandler(t)
	s := NewRedisSubscriber(0, "localhost:6379", h)
	p := NewRedisPublisher(0, "localhost:6379")

	count := 10
	channels := []string{"foo", "bar", "hi"}

	// subscribe to all channels
	for _, channel := range channels {
		if err := <-s.Subscribe(channel); err != nil {
			t.Fatal(err)
		}
	}

	// publish 10 messages to each channel
	var wg sync.WaitGroup
	wg.Add(count * len(channels))
	for _, channel := range channels {
		for i := 0; i < count; i++ {
			go func(channel string, i int) {
				if err := p.Publish(channel, []byte(strconv.Itoa(i))); err != nil {
					t.Fatal(err)
				}
				wg.Done()
			}(channel, i)
		}
	}
	wg.Wait()

	// wait for all messages
	h.waitForMessages(count * len(channels))

	// check the messages
	for _, channel := range channels {
		messages := h.messages[channel]
		for i := 0; i < count; i++ {
			if _, ok := messages[strconv.Itoa(i)]; !ok {
				t.Fatalf("%d not found from channel %s", i, channel)
			}
		}
	}

	s.Shutdown()
	p.Shutdown()
}
