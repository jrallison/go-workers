package workers

import (
	"strconv"
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

	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("increments processed stats", func() {
		rc := Config.Client

		count, _ := rc.Get("prod:stat:processed").Result()
		countInt, _ := strconv.ParseInt(count, 10, 64)
		c.Expect(countInt, Equals, int64(0))

		dayCount, _ := rc.Get("prod:stat:processed:" + time.Now().UTC().Format(layout)).Result()
		dayCountInt, _ := strconv.ParseInt(dayCount, 10, 64)
		c.Expect(dayCountInt, Equals, int64(0))

		worker.process(message)

		count, _ = rc.Get("prod:stat:processed").Result()
		countInt, _ = strconv.ParseInt(count, 10, 64)
		c.Expect(countInt, Equals, int64(1))

		dayCount, _ = rc.Get("prod:stat:processed:" + time.Now().UTC().Format(layout)).Result()
		dayCountInt, _ = strconv.ParseInt(dayCount, 10, 64)
		c.Expect(dayCountInt, Equals, int64(1))
	})

	c.Specify("failed job", func() {
		var job = (func(message *Msg) {
			panic("AHHHH")
		})

		manager := newManager("myqueue", job, 1)
		worker := newWorker(manager)

		c.Specify("increments failed stats", func() {
			rc := Config.Client

			count, _ := rc.Get("prod:stat:failed").Result()
			countInt, _ := strconv.ParseInt(count, 10, 64)
			c.Expect(countInt, Equals, int64(0))

			dayCount, _ := rc.Get("prod:stat:failed:" + time.Now().UTC().Format(layout)).Result()
			dayCountInt, _ := strconv.ParseInt(dayCount, 10, 64)
			c.Expect(dayCountInt, Equals, int64(0))

			worker.process(message)

			count, _ = rc.Get("prod:stat:failed").Result()
			countInt, _ = strconv.ParseInt(count, 10, 64)
			c.Expect(countInt, Equals, int64(1))

			dayCount, _ = rc.Get("prod:stat:failed:" + time.Now().UTC().Format(layout)).Result()
			dayCountInt, _ = strconv.ParseInt(dayCount, 10, 64)
			c.Expect(dayCountInt, Equals, int64(1))
		})
	})

	Config.Namespace = was
}
