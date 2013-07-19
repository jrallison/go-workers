package workers

import (
	"fmt"
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func ManagerSpec(c gospec.Context) {
	processed := make(chan *Msg)

	testJob := (func(message *Msg) bool {
		processed <- message
		return true
	})

	c.Specify("newManager", func() {
		c.Specify("sets queue", func() {
			manager := newManager("myqueue", testJob, 10)
			c.Expect(manager.queue, Equals, "myqueue")
		})

		c.Specify("sets job function", func() {
			manager := newManager("myqueue", testJob, 10)
			c.Expect(fmt.Sprint(manager.job), Equals, fmt.Sprint(testJob))
		})

		c.Specify("sets worker concurrency", func() {
			manager := newManager("myqueue", testJob, 10)
			c.Expect(manager.concurrency, Equals, 10)
		})
	})

	c.Specify("manage", func() {
		conn := Config.pool.Get()
		defer conn.Close()

		message, _ := NewMsg("{\"foo\":\"bar\"}")
		message2, _ := NewMsg("{\"foo\":\"bar2\"}")

		c.Specify("coordinates processing of queue messages", func() {
			manager := newManager("manager1", testJob, 10)

			conn.Do("lpush", "manager1", message.ToJson())
			conn.Do("lpush", "manager1", message2.ToJson())

			manager.start()

			c.Expect(<-processed, Equals, message)
			c.Expect(<-processed, Equals, message2)

			manager.quit()

			len, _ := redis.Int(conn.Do("llen", "manager1"))
			c.Expect(len, Equals, 0)
		})

		c.Specify("prepare stops fetching new messages from queue", func() {
			manager := newManager("manager2", testJob, 10)
			manager.start()

			manager.prepare()

			conn.Do("lpush", "manager2", message)
			conn.Do("lpush", "manager2", message2)

			manager.quit()

			len, _ := redis.Int(conn.Do("llen", "manager2"))
			c.Expect(len, Equals, 2)
		})
	})
}
