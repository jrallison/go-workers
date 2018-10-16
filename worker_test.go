package workers

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var testMiddlewareCalled bool
var failMiddlewareCalled bool

type testMiddleware struct{}

func (l *testMiddleware) Call(queue string, message *Msg, next func() error) (result error) {
	testMiddlewareCalled = true
	return next()
}

type failMiddleware struct{}

func (l *failMiddleware) Call(queue string, message *Msg, next func() error) (result error) {
	failMiddlewareCalled = true
	next()
	return errors.New("test error")
}

func confirm(manager *manager) (msg *Msg) {
	time.Sleep(10 * time.Millisecond)

	select {
	case msg = <-manager.confirm:
	default:
	}

	return
}

func TestNewWorker(t *testing.T) {
	setupTestConfig()
	var processed = make(chan *Args)

	var testJob = (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	manager := newManager("myqueue", testJob, 1)

	worker := newWorker(manager)
	assert.Equal(t, manager, worker.manager)
}

func TestWork(t *testing.T) {
	setupTestConfig()

	var processed = make(chan *Args)

	var testJob = (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	manager := newManager("myqueue", testJob, 1)

	worker := newWorker(manager)
	messages := make(chan *Msg)
	message, _ := NewMsg("{\"jid\":\"2309823\",\"args\":[\"foo\",\"bar\"]}")

	//calls job with message args
	go worker.work(messages)
	messages <- message

	args, _ := (<-processed).Array()
	<-manager.confirm

	assert.Equal(t, 2, len(args))
	assert.Equal(t, "foo", args[0])
	assert.Equal(t, "bar", args[1])

	worker.quit()

	//confirms job completed", func() {
	go worker.work(messages)
	messages <- message

	<-processed
	assert.Equal(t, message, confirm(manager))

	worker.quit()

	//runs defined middleware and confirms
	Middleware.Append(&testMiddleware{})

	go worker.work(messages)
	messages <- message

	<-processed
	assert.Equal(t, message, confirm(manager))
	assert.True(t, testMiddlewareCalled)

	worker.quit()

	Middleware = NewMiddleware(
		&MiddlewareLogging{},
		&MiddlewareRetry{},
		&MiddlewareStats{},
	)
}

func TestFailMiddleware(t *testing.T) {
	setupTestConfig()

	var processed = make(chan *Args)
	var testJob = (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	Middleware = NewMiddleware(
		&MiddlewareLogging{},
		&MiddlewareRetry{},
		&MiddlewareStats{},
	)

	//doesn't confirm if middleware cancels acknowledgement
	Middleware.Append(&failMiddleware{})

	manager := newManager("myqueue", testJob, 1)
	worker := newWorker(manager)
	messages := make(chan *Msg)
	message, _ := NewMsg("{\"jid\":\"2309823\",\"args\":[\"foo\",\"bar\"]}")

	go worker.work(messages)
	messages <- message

	<-processed
	assert.Nil(t, confirm(manager))
	assert.True(t, failMiddlewareCalled)

	worker.quit()

	Middleware = NewMiddleware(
		&MiddlewareLogging{},
		&MiddlewareRetry{},
		&MiddlewareStats{},
	)
}

func TestRecoverWithPanic(t *testing.T) {
	setupTestConfig()

	//recovers and confirms if job panics
	var panicJob = (func(message *Msg) error {
		panic(errors.New("AHHHHHHHHH"))
	})

	manager := newManager("myqueue", panicJob, 1)
	worker := newWorker(manager)

	messages := make(chan *Msg)
	message, _ := NewMsg("{\"jid\":\"2309823\",\"args\":[\"foo\",\"bar\"],\"retry\":true}")

	go worker.work(messages)
	messages <- message

	assert.Equal(t, message, confirm(manager))

	worker.quit()
}

func TestRecoverWithError(t *testing.T) {
	setupTestConfig()

	//recovers and confirms if job panics
	var panicJob = (func(message *Msg) error {
		return errors.New("AHHHHHHHHH")
	})

	manager := newManager("myqueue", panicJob, 1)
	worker := newWorker(manager)

	messages := make(chan *Msg)
	message, _ := NewMsg("{\"jid\":\"2309823\",\"args\":[\"foo\",\"bar\"],\"retry\":true}")

	go worker.work(messages)
	messages <- message

	assert.Equal(t, message, confirm(manager))

	worker.quit()
}
