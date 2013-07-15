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
		c.Specify("it puts messages from the queues on the messages channel", func() {
			fetch, _ := buildFetcher("fetchQueue2")

			conn := Config.pool.Get()
			defer conn.Close()

			conn.Do("lpush", "fetchQueue2", "test")

			message := <-fetch.Messages()

			c.Expect(message, Equals, "test")

			len, _ := redis.Int(conn.Do("llen", "fetchQueue2"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})

		c.Specify("places in progress messages on private queue", func() {
			fetch, _ := buildFetcher("fetchQueue3")

			conn := Config.pool.Get()
			defer conn.Close()

			conn.Do("lpush", "fetchQueue3", "test")

			<-fetch.Messages()

			len, _ := redis.Int(conn.Do("llen", "fetchQueue3:1:inprogress"))
			c.Expect(len, Equals, 1)

			messages, _ := redis.Strings(conn.Do("lrange", "fetchQueue3:1:inprogress", 0, -1))
			c.Expect(messages[0], Equals, "test")

			fetch.Close()
		})

		c.Specify("removes in progress message when acknowledged", func() {
			fetch, _ := buildFetcher("fetchQueue4")

			conn := Config.pool.Get()
			defer conn.Close()

			conn.Do("lpush", "fetchQueue4", "test")

			<-fetch.Messages()

			fetch.Acknowledge("test")

			len, _ := redis.Int(conn.Do("llen", "fetchQueue4:1:inprogress"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})
	})
}
