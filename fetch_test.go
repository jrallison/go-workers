package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func FetchSpec(c gospec.Context) {
	manager := newManager("myqueue", nil, 1)
	conn, _ := redis.Dial("tcp", "localhost:6379")

	c.Specify("newFetch", func() {
		c.Specify("it returns an instance of Fetch with connection to manager", func() {
			fetch := newFetch(manager).(*Fetch)
			c.Expect(fetch.manager, Equals, manager)
		})
	})

	c.Specify("Fetch", func() {
		fetch := newFetch(manager).(*Fetch)

		c.Specify("it puts messages from the queues on the messages channel", func() {
			go fetch.Fetch()

			conn.Do("lpush", "myqueue", "test")

			message := <-fetch.Messages()
			c.Expect(*message, Equals, "test")

			len, _ := redis.Int(conn.Do("llen", "myqueue"))
			c.Expect(len, Equals, 0)
		})
	})
}
