package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func buildFetch(queue string) Fetcher {
	manager := newManager(queue, nil, 1)
	fetch := manager.fetch
	go fetch.Fetch()
	return fetch
}

func FetchSpec(c gospec.Context) {
	c.Specify("Config.Fetch", func() {
		c.Specify("it returns an instance of fetch with queue", func() {
			fetch := buildFetch("fetchQueue1")
			c.Expect(fetch.Queue(), Equals, "queue:fetchQueue1")
			fetch.Close()
		})
	})

	c.Specify("Fetch", func() {
		message, _ := NewMsg("{\"foo\":\"bar\"}")

		c.Specify("it puts messages from the queues on the messages channel", func() {
			fetch := buildFetch("fetchQueue2")

			conn := Config.Pool.Get()
			defer conn.Close()

			conn.Do("lpush", "queue:fetchQueue2", message.ToJson())

			fetch.Ready() <- true
			message := <-fetch.Messages()

			c.Expect(message, Equals, message)

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue2"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})

		c.Specify("places in progress messages on private queue", func() {
			fetch := buildFetch("fetchQueue3")

			conn := Config.Pool.Get()
			defer conn.Close()

			conn.Do("lpush", "queue:fetchQueue3", message.ToJson())

			fetch.Ready() <- true
			<-fetch.Messages()

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue3:1:inprogress"))
			c.Expect(len, Equals, 1)

			messages, _ := redis.Strings(conn.Do("lrange", "queue:fetchQueue3:1:inprogress", 0, -1))
			c.Expect(messages[0], Equals, message.ToJson())

			fetch.Close()
		})

		c.Specify("removes in progress message when acknowledged", func() {
			fetch := buildFetch("fetchQueue4")

			conn := Config.Pool.Get()
			defer conn.Close()

			conn.Do("lpush", "queue:fetchQueue4", message.ToJson())

			fetch.Ready() <- true
			<-fetch.Messages()

			fetch.Acknowledge(message)

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue4:1:inprogress"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})

		c.Specify("removes in progress message when serialized differently", func() {
			json := "{\"foo\":\"bar\",\"args\":[]}"
			message, _ := NewMsg(json)

			c.Expect(json, Not(Equals), message.ToJson())

			fetch := buildFetch("fetchQueue5")

			conn := Config.Pool.Get()
			defer conn.Close()

			conn.Do("lpush", "queue:fetchQueue5", json)

			fetch.Ready() <- true
			<-fetch.Messages()

			fetch.Acknowledge(message)

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue5:1:inprogress"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})

		c.Specify("refires any messages left in progress from prior instance", func() {
			message2, _ := NewMsg("{\"foo\":\"bar2\"}")
			message3, _ := NewMsg("{\"foo\":\"bar3\"}")

			conn := Config.Pool.Get()
			defer conn.Close()

			conn.Do("lpush", "queue:fetchQueue6:1:inprogress", message.ToJson())
			conn.Do("lpush", "queue:fetchQueue6:1:inprogress", message2.ToJson())
			conn.Do("lpush", "queue:fetchQueue6", message3.ToJson())

			fetch := buildFetch("fetchQueue6")

			fetch.Ready() <- true
			c.Expect(<-fetch.Messages(), Equals, message2)
			fetch.Ready() <- true
			c.Expect(<-fetch.Messages(), Equals, message)
			fetch.Ready() <- true
			c.Expect(<-fetch.Messages(), Equals, message3)

			fetch.Acknowledge(message)
			fetch.Acknowledge(message2)
			fetch.Acknowledge(message3)

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue6:1:inprogress"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})
	})
}
