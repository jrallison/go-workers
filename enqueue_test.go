package workers

import (
	"encoding/json"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func EnqueueSpec(c gospec.Context) {
	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("Enqueue", func() {
		conn := Config.Pool.Get()
		defer conn.Close()

		c.Specify("makes the queue available", func() {
			Enqueue("enqueue1", "Add", []int{1, 2})

			found, _ := redis.Bool(conn.Do("sismember", "prod:queues", "enqueue1"))
			c.Expect(found, IsTrue)
		})

		c.Specify("adds a job to the queue", func() {
			nb, _ := redis.Int(conn.Do("llen", "prod:queue:enqueue2"))
			c.Expect(nb, Equals, 0)

			Enqueue("enqueue2", "Add", []int{1, 2})

			nb, _ = redis.Int(conn.Do("llen", "prod:queue:enqueue2"))
			c.Expect(nb, Equals, 1)
		})

		c.Specify("saves the arguments", func() {
			Enqueue("enqueue3", "Compare", []string{"foo", "bar"})

			bytes, _ := redis.Bytes(conn.Do("lpop", "prod:queue:enqueue3"))
			var result map[string]interface{}
			json.Unmarshal(bytes, &result)
			c.Expect(result["class"], Equals, "Compare")

			args := result["args"].([]interface{})
			c.Expect(len(args), Equals, 2)
			c.Expect(args[0], Equals, "foo")
			c.Expect(args[1], Equals, "bar")
		})

		c.Specify("has a jid", func() {
			Enqueue("enqueue4", "Compare", []string{"foo", "bar"})

			bytes, _ := redis.Bytes(conn.Do("lpop", "prod:queue:enqueue4"))
			var result map[string]interface{}
			json.Unmarshal(bytes, &result)
			c.Expect(result["class"], Equals, "Compare")

			jid := result["jid"].(string)
			c.Expect(len(jid), Equals, 24)
		})

		c.Specify("has enqueued_at that is close to now", func() {
			Enqueue("enqueue5", "Compare", []string{"foo", "bar"})

			bytes, _ := redis.Bytes(conn.Do("lpop", "prod:queue:enqueue5"))
			var result map[string]interface{}
			json.Unmarshal(bytes, &result)
			c.Expect(result["class"], Equals, "Compare")

			ea := result["enqueued_at"].(float64)
			c.Expect(ea, Not(Equals), 0)
			c.Expect(ea, IsWithin(0.1), nowToSecondsWithNanoPrecision())
		})

		c.Specify("has retry and retry_count when set", func() {
			EnqueueWithOptions("enqueue6", "Compare", []string{"foo", "bar"}, EnqueueOptions{RetryCount: 13, Retry: true})

			bytes, _ := redis.Bytes(conn.Do("lpop", "prod:queue:enqueue6"))
			var result map[string]interface{}
			json.Unmarshal(bytes, &result)
			c.Expect(result["class"], Equals, "Compare")

			retry := result["retry"].(bool)
			c.Expect(retry, Equals, true)

			retryCount := int(result["retry_count"].(float64))
			c.Expect(retryCount, Equals, 13)
		})
	})

	c.Specify("EnqueueIn", func() {
		scheduleQueue := "prod:" + SCHEDULED_JOBS_KEY
		conn := Config.Pool.Get()
		defer conn.Close()

		c.Specify("has added a job in the scheduled queue", func() {
			_, err := EnqueueIn("enqueuein1", "Compare", 10, map[string]interface{}{"foo": "bar"})
			c.Expect(err, Equals, nil)

			scheduledCount, _ := redis.Int(conn.Do("zcard", scheduleQueue))
			c.Expect(scheduledCount, Equals, 1)

			conn.Do("del", scheduleQueue)
		})

		c.Specify("has the correct 'queue'", func() {
			_, err := EnqueueIn("enqueuein2", "Compare", 10, map[string]interface{}{"foo": "bar"})
			c.Expect(err, Equals, nil)

			var data EnqueueData
			elem, err := conn.Do("zrange", scheduleQueue, 0, -1)
			bytes, err := redis.Bytes(elem.([]interface{})[0], err)
			json.Unmarshal(bytes, &data)

			c.Expect(data.Queue, Equals, "enqueuein2")

			conn.Do("del", scheduleQueue)
		})
	})

	Config.Namespace = was
}
