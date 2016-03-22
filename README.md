go-redis-pubsub
==================

This library is a specialized Go client for Redis pubsub. I'm using it to support real-time high-throughput messaging between server instances when the expected channel count is large and very dynamic.

It's implemented using [redigo](https://github.com/garyburd/redigo).

### Features

- Manages a subscription pool on behalf of the user to allow for concurrent channel subscription processing and automatic reconnection with exponential backoff.
- Attempts to mitigate `SUBSCRIBE` / `UNSUBSCRIBE` thrash by delaying unsubscription via a timer mechanism.
- Manages a send buffer and worker pool with the help of the `redis.Pool` implementation.
- Provides a handler callback interface for both subscribers and publishers.
