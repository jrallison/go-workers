package workers

import (
	"time"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func MiddlewareRetrySpec(c gospec.Context) {
	var panicingJob = (func(message Msgs) {
		panic("AHHHH")
	})

	var wares = NewMiddleware(
		&MiddlewareRetry{},
	)

	layout := "2006-01-02 15:04:05 MST"
	manager := newManager("myqueue", panicingJob, 1)
	worker := newWorker(manager)

	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("puts messages in retry queue when they fail", func() {
		messages, _ := NewMsgs([]string{"{\"jid\":\"2\",\"retry\":true}"})

		wares.call("myqueue", messages, func() {
			worker.process(messages)
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		c.Expect(retries[0], Equals, messages[0].ToJson())
	})

	c.Specify("allows disabling retries", func() {
		messages, _ := NewMsgs([]string{"{\"jid\":\"2\",\"retry\":false}"})

		wares.call("myqueue", messages, func() {
			worker.process(messages)
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", "prod:"+RETRY_KEY))
		c.Expect(count, Equals, 0)
	})

	c.Specify("doesn't retry by default", func() {
		messages, _ := NewMsgs([]string{"{\"jid\":\"2\"}"})

		wares.call("myqueue", messages, func() {
			worker.process(messages)
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", "prod:"+RETRY_KEY))
		c.Expect(count, Equals, 0)
	})

	c.Specify("allows numeric retries", func() {
		messages, _ := NewMsgs([]string{"{\"jid\":\"2\",\"retry\":5}"})

		wares.call("myqueue", messages, func() {
			worker.process(messages)
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		c.Expect(retries[0], Equals, messages[0].ToJson())
	})

	c.Specify("handles new failed message", func() {
		messages, _ := NewMsgs([]string{"{\"jid\":\"2\",\"retry\":true}"})

		wares.call("myqueue", messages, func() {
			worker.process(messages)
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		messages, _ = NewMsgs([]string{retries[0]})

		c.Expect(messages[0].queue, Equals, "myqueue")
		c.Expect(messages[0].error, Equals, "AHHHH")
		//c.Expect(messages[0].errorClass, Equals, "")
		c.Expect(messages[0].retryCount, Equals, 0)
		//c.Expect(messages[0].errorBacktrace, Equals, "")
		c.Expect(messages[0].failedAt, Equals, time.Now().UTC().Format(layout))
	})

	c.Specify("handles recurring failed message", func() {
		messages, _ := NewMsgs([]string{"{\"jid\":\"2\",\"retry\":true,\"queue\":\"default\",\"error_message\":\"bam\",\"failed_at\":\"2013-07-20 14:03:42 UTC\",\"retry_count\":10}"})

		wares.call("myqueue", messages, func() {
			worker.process(messages)
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		messages, _ = NewMsgs(retries)

		c.Expect(messages[0].queue, Equals, "myqueue")
		c.Expect(messages[0].error, Equals, "AHHHH")
		c.Expect(messages[0].retryCount, Equals, 11)
		c.Expect(messages[0].failedAt, Equals, "2013-07-20 14:03:42 UTC")
		c.Expect(messages[0].retriedAt, Equals, time.Now().UTC().Format(layout))
	})

	c.Specify("handles recurring failed message with customized max", func() {
		messages, _ := NewMsgs([]string{"{\"jid\":\"2\",\"retry\":10,\"queue\":\"default\",\"error_message\":\"bam\",\"failed_at\":\"2013-07-20 14:03:42 UTC\",\"retry_count\":8}"})

		wares.call("myqueue", messages, func() {
			worker.process(messages)
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		retries, _ := redis.Strings(conn.Do("zrange", "prod:"+RETRY_KEY, 0, 1))
		messages, _ = NewMsgs(retries)

		c.Expect(messages[0].queue, Equals, "myqueue")
		c.Expect(messages[0].error, Equals, "AHHHH")
		c.Expect(messages[0].retryCount, Equals, 9)
		c.Expect(messages[0].failedAt, Equals, "2013-07-20 14:03:42 UTC")
		c.Expect(messages[0].retriedAt, Equals, time.Now().UTC().Format(layout))
	})

	c.Specify("doesn't retry after default number of retries", func() {
		messages, _ := NewMsgs([]string{"{\"jid\":\"2\",\"retry\":true,\"retry_count\":25}"})

		wares.call("myqueue", messages, func() {
			worker.process(messages)
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", "prod:"+RETRY_KEY))
		c.Expect(count, Equals, 0)
	})

	c.Specify("doesn't retry after customized number of retries", func() {
		messages, _ := NewMsgs([]string{"{\"jid\":\"2\",\"retry\":3,\"retry_count\":3}"})

		wares.call("myqueue", messages, func() {
			worker.process(messages)
		})

		conn := Config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("zcard", "prod:"+RETRY_KEY))
		c.Expect(count, Equals, 0)
	})

	Config.Namespace = was
}
