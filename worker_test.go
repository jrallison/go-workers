package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

var processed = make([]string, 0)

func WorkerTestJob(message interface{}) bool {
	processed = append(processed, message.(string))
	return true
}

func WorkerSpec(c gospec.Context) {
	manager := newManager("myqueue", WorkerTestJob, 1)

	c.Specify("newWorker", func() {
		c.Specify("it returns an instance of worker with connection to manager", func() {
			worker := newWorker(manager)
			c.Expect(worker.manager, Equals, manager)
		})
	})

	c.Specify("work", func() {
		worker := newWorker(manager)

		c.Specify("gives each message to a new job instance, and calls perform", func() {
			messages := make(chan interface{})

			go worker.work(messages)

			messages <- "test"

			c.Expect(len(processed), Equals, 1)
			c.Expect(processed[0], Equals, "test")
		})
	})
}
