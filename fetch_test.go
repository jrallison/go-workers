package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func FetchSpec(c gospec.Context) {
	manager := newManager("fetchQueue", nil, 1)
	// Load test instance of redis on port 6400
	conn, _ := redis.Dial("tcp", "localhost:6400")

	c.Specify("newFetch", func() {
		c.Specify("it returns an instance of fetch with connection to manager", func() {
			fetch := newFetch(manager).(*fetch)
			c.Expect(fetch.manager, Equals, manager)
		})
	})

	c.Specify("Fetch", func() {
		fetch := newFetch(manager).(*fetch)

		c.Specify("it puts messages from the queues on the messages channel", func() {

			go fetch.Fetch()

			conn.Do("lpush", "fetchQueue", "test")

			message := <-fetch.Messages()

			c.Expect(message, Equals, "test")

			len, _ := redis.Int(conn.Do("llen", "fetchQueue"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})
	})
}
