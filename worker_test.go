package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

var middlewareCalled bool

type testMiddleware struct{}

func (l *testMiddleware) Call(message interface{}, next func()) {
	middlewareCalled = true
	next()
}

func WorkerSpec(c gospec.Context) {
	var processed = make([]*Msg, 0)
	middlewareCalled = false

	var testJob = (func(message *Msg) bool {
		processed = append(processed, message)
		return true
	})

	manager := newManager("myqueue", testJob, 1)

	c.Specify("newWorker", func() {
		c.Specify("it returns an instance of worker with connection to manager", func() {
			worker := newWorker(manager)
			c.Expect(worker.manager, Equals, manager)
		})
	})

	c.Specify("work", func() {
		worker := newWorker(manager)
		messages := make(chan *Msg)
		message, _ := NewMsg("{\"foo\":\"bar\"}")

		c.Specify("gives each message to a new job instance, and calls perform", func() {
			go worker.work(messages)
			messages <- message

			c.Expect(len(processed), Equals, 1)
			c.Expect(processed, Contains, message)

			close(messages)
		})

		c.Specify("confirms job completed", func() {
			go worker.work(messages)
			messages <- message

			c.Expect(<-manager.confirm, Equals, message)

			close(messages)
		})

		c.Specify("runs defined middleware", func() {
			Middleware.Append(&testMiddleware{})

			go worker.work(messages)
			messages <- message

			c.Expect(middlewareCalled, IsTrue)

			close(messages)
		})
	})
}
