package workers

import (
	"context"
	"encoding/json"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

func EnqueueSpec(c gospec.Context) {
	was := Config.Namespace
	Config.Namespace = "{worker}:"

	c.Specify("Enqueue", func() {

		c.Specify("makes the queue available", func() {
			Enqueue("enqueue1", "Add", []int{1, 2})

			found, _ := Config.Redis.SIsMember(context.Background(), "{worker}:queues", "enqueue1").Result()
			c.Expect(found, IsTrue)
		})

		c.Specify("adds a job to the queue", func() {
			nb, _ := Config.Redis.LLen(context.Background(), "{worker}:queue:enqueue2").Result()
			c.Expect(nb, Equals, int64(0))

			Enqueue("enqueue2", "Add", []int{1, 2})

			nb, _ = Config.Redis.LLen(context.Background(), "{worker}:queue:enqueue2").Result()
			c.Expect(nb, Equals, int64(1))
		})

		c.Specify("saves the arguments", func() {
			Enqueue("enqueue3", "Compare", []string{"foo", "bar"})

			bytes, _ := Config.Redis.LPop(context.Background(), "{worker}:queue:enqueue3").Result()
			var result map[string]interface{}
			json.Unmarshal([]byte(bytes), &result)
			c.Expect(result["class"], Equals, "Compare")

			args := result["args"].([]interface{})
			c.Expect(len(args), Equals, 2)
			c.Expect(args[0], Equals, "foo")
			c.Expect(args[1], Equals, "bar")
		})

		c.Specify("has a jid", func() {
			Enqueue("enqueue4", "Compare", []string{"foo", "bar"})

			bytes, _ := Config.Redis.LPop(context.Background(), "{worker}:queue:enqueue4").Result()
			var result map[string]interface{}
			json.Unmarshal([]byte(bytes), &result)
			c.Expect(result["class"], Equals, "Compare")

			jid := result["jid"].(string)
			c.Expect(len(jid), Equals, 24)
		})

		c.Specify("has enqueued_at that is close to now", func() {
			Enqueue("enqueue5", "Compare", []string{"foo", "bar"})

			bytes, _ := Config.Redis.LPop(context.Background(), "{worker}:queue:enqueue5").Result()
			var result map[string]interface{}
			json.Unmarshal([]byte(bytes), &result)
			c.Expect(result["class"], Equals, "Compare")

			ea := result["enqueued_at"].(float64)
			c.Expect(ea, Not(Equals), 0)
			c.Expect(ea, IsWithin(0.1), nowToSecondsWithNanoPrecision())
		})

		c.Specify("has retry and retry_count when set", func() {
			EnqueueWithOptions("enqueue6", "Compare", []string{"foo", "bar"}, EnqueueOptions{RetryCount: 13, Retry: true})

			bytes, _ := Config.Redis.LPop(context.Background(), "{worker}:queue:enqueue6").Result()
			var result map[string]interface{}
			json.Unmarshal([]byte(bytes), &result)
			c.Expect(result["class"], Equals, "Compare")

			retry := result["retry"].(bool)
			c.Expect(retry, Equals, true)

			retryCount := int(result["retry_count"].(float64))
			c.Expect(retryCount, Equals, 13)
		})
	})

	c.Specify("EnqueueIn", func() {
		scheduleQueue := "{worker}:" + SCHEDULED_JOBS_KEY

		c.Specify("has added a job in the scheduled queue", func() {
			_, err := EnqueueIn("enqueuein1", "Compare", 10, map[string]interface{}{"foo": "bar"})
			c.Expect(err, Equals, nil)

			scheduledCount, _ := Config.Redis.ZCard(context.Background(), scheduleQueue).Result()
			c.Expect(scheduledCount, Equals, int64(1))

			Config.Redis.Del(context.Background(), scheduleQueue)
		})

		c.Specify("has the correct 'queue'", func() {
			_, err := EnqueueIn("enqueuein2", "Compare", 10, map[string]interface{}{"foo": "bar"})
			c.Expect(err, Equals, nil)

			var data EnqueueData
			elem, err := Config.Redis.ZRange(context.Background(), scheduleQueue, 0, -1).Result()
			c.Expect(err, Equals, nil)
			json.Unmarshal([]byte(elem[0]), &data)

			c.Expect(data.Queue, Equals, "enqueuein2")

			Config.Redis.Del(context.Background(), scheduleQueue)
		})
	})

	Config.Namespace = was
}
