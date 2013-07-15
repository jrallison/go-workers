package workers

import (
	"fmt"
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func ManagerSpec(c gospec.Context) {
	processed := make(chan string)

	testJob := (func(message interface{}) bool {
		processed <- message.(string)
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

		c.Specify("coordinates processing of queue messages", func() {
			manager := newManager("manager1", testJob, 10)

			conn.Do("lpush", "manager1", "test")
			conn.Do("lpush", "manager1", "test2")

			manager.start()

			c.Expect(<-processed, Equals, "test")
			c.Expect(<-processed, Equals, "test2")

			manager.quit()

			len, _ := redis.Int(conn.Do("llen", "manager1"))
			c.Expect(len, Equals, 0)
		})

		c.Specify("prepare stops fetching new messages from queue", func() {
			manager := newManager("manager2", testJob, 10)
			manager.start()

			manager.prepare()

			conn.Do("lpush", "manager2", "testa")
			conn.Do("lpush", "manager2", "testb")

			manager.quit()

			len, _ := redis.Int(conn.Do("llen", "manager2"))
			c.Expect(len, Equals, 2)
		})
	})
}
