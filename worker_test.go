package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"

	"time"
)

var testMiddlewareCalled bool
var failMiddlewareCalled bool

type testMiddleware struct{}

func (l *testMiddleware) Call(queue string, messages Msgs, next func() bool) (result bool) {
	testMiddlewareCalled = true
	return next()
}

type failMiddleware struct{}

func (l *failMiddleware) Call(queue string, messages Msgs, next func() bool) (result bool) {
	failMiddlewareCalled = true
	next()
	return false
}

func confirm(manager *manager) (msgs Msgs) {
	time.Sleep(10 * time.Millisecond)

	select {
	case msgs = <-manager.confirm:
	default:
	}

	return
}

func WorkerSpec(c gospec.Context) {
	var processed = make(chan *Args)

	var testJob = (func(messages Msgs) {
		for _, m := range messages {
			processed <- m.Args()
		}
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
		msgs := make(chan Msgs)

		messages := buildVirginMessages("[\"foo\",\"bar\"]")
		messages[0].jid = "2309823"

		c.Specify("calls job with message args", func() {
			go worker.work(msgs)
			msgs <- messages

			args, _ := (<-processed).Array()
			<-manager.confirm

			c.Expect(len(args), Equals, 2)
			c.Expect(args[0], Equals, "foo")
			c.Expect(args[1], Equals, "bar")

			worker.quit()
		})

		c.Specify("confirms job completed", func() {
			go worker.work(msgs)
			msgs <- messages

			<-processed

			found := confirm(manager)
			c.Expect(len(found), Equals, 1)
			c.Expect(found[0].original, Equals, messages[0].original)

			worker.quit()
		})

		c.Specify("runs defined middleware and confirms", func() {
			Middleware.Append(&testMiddleware{})

			go worker.work(msgs)
			msgs <- messages

			<-processed

			found := confirm(manager)
			c.Expect(len(found), Equals, 1)
			c.Expect(found[0].original, Equals, messages[0].original)

			c.Expect(testMiddlewareCalled, IsTrue)

			worker.quit()

			Middleware = NewMiddleware(
				&MiddlewareLogging{},
				&MiddlewareRetry{},
				&MiddlewareStats{},
			)
		})

		c.Specify("doesn't confirm if middleware cancels acknowledgement", func() {
			Middleware.Append(&failMiddleware{})

			go worker.work(msgs)
			msgs <- messages

			<-processed

			found := confirm(manager)
			c.Expect(len(found), Equals, 0)

			c.Expect(failMiddlewareCalled, IsTrue)

			worker.quit()

			Middleware = NewMiddleware(
				&MiddlewareLogging{},
				&MiddlewareRetry{},
				&MiddlewareStats{},
			)
		})

		c.Specify("recovers and confirms if job panics", func() {
			var panicJob = (func(messages Msgs) {
				panic("AHHHHHHHHH")
			})

			manager := newManager("myqueue", panicJob, 1)
			worker := newWorker(manager)

			go worker.work(msgs)
			msgs <- messages

			found := confirm(manager)
			c.Expect(len(found), Equals, 1)
			c.Expect(found[0].original, Equals, messages[0].original)

			worker.quit()
		})
	})
}
