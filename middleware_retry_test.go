package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func MiddlewareRetrySpec(c gospec.Context) {
	var panicingJob = (func(args *Args) {
		panic("AHHHH")
	})

	var wares = newMiddleware(
		&MiddlewareRetry{},
	)

	manager := newManager("myqueue", panicingJob, 1)
	worker := newWorker(manager)

	c.Specify("puts messages in retry queue when they fail", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

		wares.call("myqueue", message, func() {
			worker.process(message)
		})

		conn := Config.pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "goretry", 0, 1))
		c.Expect(retries[0], Equals, message.ToJson())
	})

	c.Specify("allows disabling retries", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":false}")

		wares.call("myqueue", message, func() {
			worker.process(message)
		})

		conn := Config.pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", "goretry"))
		c.Expect(count, Equals, 0)
	})

	c.Specify("doesn't retry by default", func() {
		message, _ := NewMsg("{\"jid\":\"2\"}")

		wares.call("myqueue", message, func() {
			worker.process(message)
		})

		conn := Config.pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", "goretry"))
		c.Expect(count, Equals, 0)
	})

	c.Specify("allows numeric retries", func() {
		message, _ := NewMsg("{\"jid\":\"2\",\"retry\":5}")

		wares.call("myqueue", message, func() {
			worker.process(message)
		})

		conn := Config.pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "goretry", 0, 1))
		c.Expect(retries[0], Equals, message.ToJson())
	})

	c.Specify("handles new failed message", func() {})
	c.Specify("allows a retry queue", func() {})
	c.Specify("handles recurring failed message", func() {})
	c.Specify("handles recurring failed message with customized max", func() {})
	c.Specify("doesn't retry after default number of retries", func() {})
	c.Specify("doesn't retry after customized number of retries", func() {})
}
