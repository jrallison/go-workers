package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

var middlewareCalled bool

type testMiddleware struct{}

func (l *testMiddleware) Call(queue string, message *Msg, next func()) {
	middlewareCalled = true
	next()
}

func WorkerSpec(c gospec.Context) {
	var processed = make(chan *Args)
	middlewareCalled = false

	var testJob = (func(args *Args) {
		processed <- args
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
		message, _ := NewMsg("{\"jid\":\"2309823\",\"args\":[\"foo\",\"bar\"]}")

		c.Specify("calls job with message args", func() {
			go worker.work(messages)
			messages <- message

			args, _ := (<-processed).Array()
			<-manager.confirm

			c.Expect(len(args), Equals, 2)
			c.Expect(args[0], Equals, "foo")
			c.Expect(args[1], Equals, "bar")

			worker.quit()
		})

		c.Specify("confirms job completed", func() {
			go worker.work(messages)
			messages <- message

			<-processed
			c.Expect(<-manager.confirm, Equals, message)

			worker.quit()
		})

		c.Specify("runs defined middleware", func() {
			Middleware.Append(&testMiddleware{})

			go worker.work(messages)
			messages <- message

			<-processed
			<-manager.confirm

			c.Expect(middlewareCalled, IsTrue)

			worker.quit()
		})

		c.Specify("recovers and confirms if job panics", func() {
			var panicJob = (func(args *Args) {
				panic("AHHHHHHHHH")
			})

			manager := newManager("myqueue", panicJob, 1)
			worker := newWorker(manager)

			go worker.work(messages)
			messages <- message
			c.Expect(<-manager.confirm, Equals, message)

			worker.quit()
		})
	})
}
