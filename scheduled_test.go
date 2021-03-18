package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/go-redis/redis"
)

func ScheduledSpec(c gospec.Context) {
	scheduled := newScheduled(RETRY_KEY)

	was := Config.Namespace
	Config.Namespace = "{worker}:"

	c.Specify("empties retry queues up to the current time", func() {
		now := nowToSecondsWithNanoPrecision()

		message1, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar1\"}")
		message2, _ := NewMsg("{\"queue\":\"myqueue\",\"foo\":\"bar2\"}")
		message3, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar3\"}")

		Config.Redis.ZAdd("{worker}:"+RETRY_KEY, redis.Z{
			Member: message1.ToJson(),
			Score:  now - 60.0,
		})
		Config.Redis.ZAdd("{worker}:"+RETRY_KEY, redis.Z{
			Member: message2.ToJson(),
			Score:  now - 10.0,
		})
		Config.Redis.ZAdd("{worker}:"+RETRY_KEY, redis.Z{
			Member: message3.ToJson(),
			Score:  now + 60.0,
		})

		scheduled.poll()

		defaultCount, _ := Config.Redis.LLen("{worker}:queue:default").Result()
		myqueueCount, _ := Config.Redis.LLen("{worker}:queue:myqueue").Result()
		pending, _ := Config.Redis.ZCard("{worker}:" + RETRY_KEY).Result()

		c.Expect(defaultCount, Equals, int64(1))
		c.Expect(myqueueCount, Equals, int64(1))
		c.Expect(pending, Equals, int64(1))
	})

	Config.Namespace = was
}
