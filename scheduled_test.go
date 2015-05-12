package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func ScheduledSpec(c gospec.Context) {
	scheduled := newScheduled(RETRY_KEY)

	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("empties retry queues up to the current time", func() {
		conn := Config.Pool.Get()
		defer conn.Close()

		now := nowToSecondsWithNanoPrecision()

		message1, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar1\"}")
		message2, _ := NewMsg("{\"queue\":\"myqueue\",\"foo\":\"bar2\"}")
		message3, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar3\"}")

		conn.Do("zadd", "prod:"+RETRY_KEY, now-60.0, message1.ToJson())
		conn.Do("zadd", "prod:"+RETRY_KEY, now-10.0, message2.ToJson())
		conn.Do("zadd", "prod:"+RETRY_KEY, now+60.0, message3.ToJson())

		scheduled.poll()

		defaultCount, _ := redis.Int(conn.Do("llen", "prod:queue:default"))
		myqueueCount, _ := redis.Int(conn.Do("llen", "prod:queue:myqueue"))
		pending, _ := redis.Int(conn.Do("zcard", "prod:"+RETRY_KEY))

		c.Expect(defaultCount, Equals, 1)
		c.Expect(myqueueCount, Equals, 1)
		c.Expect(pending, Equals, 1)
	})

	Config.Namespace = was
}
