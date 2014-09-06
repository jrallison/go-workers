package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
	"time"
)

func MiddlewareStatsSpec(c gospec.Context) {
	var job = (func(message *Msg) {
		// noop
	})

	layout := "2006-01-02"
	manager := newManager("myqueue", job, 1)
	worker := newWorker(manager)
	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("increments processed stats", func() {
		conn := Config.Pool.Get()
		defer conn.Close()

		count, _ := redis.Int(conn.Do("get", "prod:stat:processed"))
		dayCount, _ := redis.Int(conn.Do("get", "prod:stat:processed:"+time.Now().UTC().Format(layout)))

		c.Expect(count, Equals, 0)
		c.Expect(dayCount, Equals, 0)

		worker.process(message)

		count, _ = redis.Int(conn.Do("get", "prod:stat:processed"))
		dayCount, _ = redis.Int(conn.Do("get", "prod:stat:processed:"+time.Now().UTC().Format(layout)))

		c.Expect(count, Equals, 1)
		c.Expect(dayCount, Equals, 1)
	})

	c.Specify("failed job", func() {
		var job = (func(message *Msg) {
			panic("AHHHH")
		})

		manager := newManager("myqueue", job, 1)
		worker := newWorker(manager)

		c.Specify("increments failed stats", func() {
			conn := Config.Pool.Get()
			defer conn.Close()

			count, _ := redis.Int(conn.Do("get", "prod:stat:failed"))
			dayCount, _ := redis.Int(conn.Do("get", "prod:stat:failed:"+time.Now().UTC().Format(layout)))

			c.Expect(count, Equals, 0)
			c.Expect(dayCount, Equals, 0)

			worker.process(message)

			count, _ = redis.Int(conn.Do("get", "prod:stat:failed"))
			dayCount, _ = redis.Int(conn.Do("get", "prod:stat:failed:"+time.Now().UTC().Format(layout)))

			c.Expect(count, Equals, 1)
			c.Expect(dayCount, Equals, 1)
		})
	})

	Config.Namespace = was
}
