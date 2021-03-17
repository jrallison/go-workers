package workers

import (
	"time"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

func MiddlewareStatsSpec(c gospec.Context) {
	var job = (func(message *Msg) {
		// noop
	})

	layout := "2006-01-02"
	manager := newManager("myqueue", job, 1)
	worker := newWorker(manager)
	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

	Config.Namespace = "{worker}:"
	was := Config.Namespace

	c.Specify("increments processed stats", func() {
		count, _ := Config.Redis.Get("{worker}:stat:processed").Result()
		dayCount, _ := Config.Redis.Get("{worker}:stat:processed:" + time.Now().UTC().Format(layout)).Result()

		c.Expect(count, Equals, "")
		c.Expect(dayCount, Equals, "")

		worker.process(message)

		count, _ = Config.Redis.Get("{worker}:stat:processed").Result()
		dayCount, _ = Config.Redis.Get("{worker}:stat:processed:" + time.Now().UTC().Format(layout)).Result()

		c.Expect(count, Equals, "1")
		c.Expect(dayCount, Equals, "1")
	})

	c.Specify("failed job", func() {
		var job = (func(message *Msg) {
			panic("AHHHH")
		})

		manager := newManager("myqueue", job, 1)
		worker := newWorker(manager)

		c.Specify("increments failed stats", func() {
			count, _ := Config.Redis.Get("{worker}:stat:failed").Result()
			dayCount, _ := Config.Redis.Get("{worker}:stat:failed:" + time.Now().UTC().Format(layout)).Result()

			c.Expect(count, Equals, "")
			c.Expect(dayCount, Equals, "")

			worker.process(message)

			count, _ = Config.Redis.Get("{worker}:stat:failed").Result()
			dayCount, _ = Config.Redis.Get("{worker}:stat:failed:" + time.Now().UTC().Format(layout)).Result()

			c.Expect(count, Equals, "1")
			c.Expect(dayCount, Equals, "1")
		})
	})

	Config.Namespace = was
}
