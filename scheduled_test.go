package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
	"time"
)

func ScheduledSpec(c gospec.Context) {
	scheduled := newScheduled("goretry")

	c.Specify("empties retry queues up to the current time", func() {
		conn := Config.pool.Get()
		defer conn.Close()

		now := time.Now().Unix()

		message1, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar1\"}")
		message2, _ := NewMsg("{\"queue\":\"myqueue\",\"foo\":\"bar2\"}")
		message3, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar3\"}")

		conn.Do("zadd", "goretry", now-60, message1.ToJson())
		conn.Do("zadd", "goretry", now-10, message2.ToJson())
		conn.Do("zadd", "goretry", now+60, message3.ToJson())

		scheduled.poll(false)

		defaultCount, _ := redis.Int(conn.Do("llen", "queue:default"))
		myqueueCount, _ := redis.Int(conn.Do("llen", "queue:myqueue"))
		pending, _ := redis.Int(conn.Do("zcard", "goretry"))

		c.Expect(defaultCount, Equals, 1)
		c.Expect(myqueueCount, Equals, 1)
		c.Expect(pending, Equals, 1)
	})
}
