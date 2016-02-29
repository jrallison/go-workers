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

		queue, _ := messages[0].Get("queue").String()
		error_message, _ := messages[0].Get("error_message").String()
		error_class, _ := messages[0].Get("error_class").String()
		retry_count, _ := messages[0].Get("retry_count").Int()
		error_backtrace, _ := messages[0].Get("error_backtrace").String()
		failed_at, _ := messages[0].Get("failed_at").String()

		c.Expect(queue, Equals, "myqueue")
		c.Expect(error_message, Equals, "AHHHH")
		c.Expect(error_class, Equals, "")
		c.Expect(retry_count, Equals, 0)
		c.Expect(error_backtrace, Equals, "")
		c.Expect(failed_at, Equals, time.Now().UTC().Format(layout))
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

		queue, _ := messages[0].Get("queue").String()
		error_message, _ := messages[0].Get("error_message").String()
		retry_count, _ := messages[0].Get("retry_count").Int()
		failed_at, _ := messages[0].Get("failed_at").String()
		retried_at, _ := messages[0].Get("retried_at").String()

		c.Expect(queue, Equals, "myqueue")
		c.Expect(error_message, Equals, "AHHHH")
		c.Expect(retry_count, Equals, 11)
		c.Expect(failed_at, Equals, "2013-07-20 14:03:42 UTC")
		c.Expect(retried_at, Equals, time.Now().UTC().Format(layout))
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

		queue, _ := messages[0].Get("queue").String()
		error_message, _ := messages[0].Get("error_message").String()
		retry_count, _ := messages[0].Get("retry_count").Int()
		failed_at, _ := messages[0].Get("failed_at").String()
		retried_at, _ := messages[0].Get("retried_at").String()

		c.Expect(queue, Equals, "myqueue")
		c.Expect(error_message, Equals, "AHHHH")
		c.Expect(retry_count, Equals, 9)
		c.Expect(failed_at, Equals, "2013-07-20 14:03:42 UTC")
		c.Expect(retried_at, Equals, time.Now().UTC().Format(layout))
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
