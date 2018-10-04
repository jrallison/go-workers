package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/go-redis/redis"
)

func ScheduledSpec(c gospec.Context) {
	scheduled := newScheduled(RETRY_KEY)

	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("empties retry queues up to the current time", func() {
		rc := Config.Client

		now := nowToSecondsWithNanoPrecision()

		message1, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar1\"}")
		message2, _ := NewMsg("{\"queue\":\"myqueue\",\"foo\":\"bar2\"}")
		message3, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar3\"}")

		rc.ZAdd("prod:"+RETRY_KEY, redis.Z{Score: now - 60.0, Member: message1.ToJson()}).Result()
		rc.ZAdd("prod:"+RETRY_KEY, redis.Z{Score: now - 10.0, Member: message2.ToJson()}).Result()
		rc.ZAdd("prod:"+RETRY_KEY, redis.Z{Score: now + 60.0, Member: message3.ToJson()}).Result()

		scheduled.poll()

		defaultCount, _ := rc.LLen("prod:queue:default").Result()
		myqueueCount, _ := rc.LLen("prod:queue:myqueue").Result()
		pending, _ := rc.ZCard("prod:" + RETRY_KEY).Result()

		c.Expect(defaultCount, Equals, int64(1))
		c.Expect(myqueueCount, Equals, int64(1))
		c.Expect(pending, Equals, int64(1))
	})

	Config.Namespace = was
}
