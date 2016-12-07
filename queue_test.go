package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

func QueueSpec(c gospec.Context) {
	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("QueueSize", func() {
		c.Specify("returns the actual size of the queue", func() {
			for i := 1; i <= 10; i++ {
				Enqueue("queue1", "Compare", []string{"foo", "bar"})
				queueSize, err := QueueSize("queue1")
				c.Expect(err, Equals, nil)
				c.Expect(queueSize, Equals, i)
			}
		})
	})

	c.Specify("ScheduledQueueSize", func() {
		c.Specify("returns the number of scheduled jobs", func() {
			for i := 1; i <= 10; i++ {
				EnqueueIn("queue2", "Compare", 10, []string{"foo", "bar"})
				queueSize, err := ScheduledQueueSize()
				c.Expect(err, Equals, nil)
				c.Expect(queueSize, Equals, i)
			}
		})
	})

	Config.Namespace = was
}
