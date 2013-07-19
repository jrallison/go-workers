package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func FetchSpec(c gospec.Context) {
	var buildFetcher = func(queue string) (*fetch, *manager) {
		manager := newManager(queue, nil, 1)
		fetch := newFetch(manager).(*fetch)
		go fetch.Fetch()
		return fetch, manager
	}

	c.Specify("newFetch", func() {
		c.Specify("it returns an instance of fetch with connection to manager", func() {
			fetch, manager := buildFetcher("fetchQueue1")
			c.Expect(fetch.manager, Equals, manager)
			fetch.Close()
		})
	})

	c.Specify("Fetch", func() {
		message, _ := NewMsg("{\"foo\":\"bar\"}")

		c.Specify("it puts messages from the queues on the messages channel", func() {
			fetch, _ := buildFetcher("fetchQueue2")

			conn := Config.pool.Get()
			defer conn.Close()

			conn.Do("lpush", "fetchQueue2", message.ToJson())

			message := <-fetch.Messages()

			c.Expect(message, Equals, message)

			len, _ := redis.Int(conn.Do("llen", "fetchQueue2"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})

		c.Specify("places in progress messages on private queue", func() {
			fetch, _ := buildFetcher("fetchQueue3")

			conn := Config.pool.Get()
			defer conn.Close()

			conn.Do("lpush", "fetchQueue3", message.ToJson())

			<-fetch.Messages()

			len, _ := redis.Int(conn.Do("llen", "fetchQueue3:1:inprogress"))
			c.Expect(len, Equals, 1)

			messages, _ := redis.Strings(conn.Do("lrange", "fetchQueue3:1:inprogress", 0, -1))
			c.Expect(messages[0], Equals, message.ToJson())

			fetch.Close()
		})

		c.Specify("removes in progress message when acknowledged", func() {
			fetch, _ := buildFetcher("fetchQueue4")

			conn := Config.pool.Get()
			defer conn.Close()

			conn.Do("lpush", "fetchQueue4", message.ToJson())

			<-fetch.Messages()

			fetch.Acknowledge(message)

			len, _ := redis.Int(conn.Do("llen", "fetchQueue4:1:inprogress"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})

		c.Specify("refires any messages left in progress from prior instance", func() {
			message2, _ := NewMsg("{\"foo\":\"bar2\"}")
			message3, _ := NewMsg("{\"foo\":\"bar3\"}")

			conn := Config.pool.Get()
			defer conn.Close()

			conn.Do("lpush", "fetchQueue5:1:inprogress", message.ToJson())
			conn.Do("lpush", "fetchQueue5:1:inprogress", message2.ToJson())
			conn.Do("lpush", "fetchQueue5", message3.ToJson())

			fetch, _ := buildFetcher("fetchQueue5")

			c.Expect((<-fetch.Messages()).ToJson(), Equals, message2.ToJson())
			c.Expect((<-fetch.Messages()).ToJson(), Equals, message.ToJson())
			c.Expect((<-fetch.Messages()).ToJson(), Equals, message3.ToJson())

			fetch.Acknowledge(message)
			fetch.Acknowledge(message2)
			fetch.Acknowledge(message3)

			len, _ := redis.Int(conn.Do("llen", "fetchQueue5:1:inprogress"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})
	})
}
