package workers

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type customMid struct {
	trace []string
	Base  string
	mutex sync.Mutex
}

func (m *customMid) Call(queue string, message *Msg, next func() error) (err error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.trace = append(m.trace, m.Base+"1")
	err = next()
	m.trace = append(m.trace, m.Base+"2")
	return
}

func (m *customMid) Trace() []string {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	t := make([]string, len(m.trace))
	copy(t, m.trace)

	return t
}

func TestNewManager(t *testing.T) {
	namespace := "prod"
	setupTestConfigWithNamespace(namespace)

	processed := make(chan *Args)
	testJob := (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	//sets queue with namespace
	manager := newManager("myqueue", testJob, 10)
	assert.Equal(t, "prod:queue:myqueue", manager.queue)

	//sets job function
	manager = newManager("myqueue", testJob, 10)

	f1 := reflect.ValueOf(manager.job)
	f2 := reflect.ValueOf(testJob)
	assert.Equal(t, f1.Pointer(), f2.Pointer())

	//sets worker concurrency", func() {
	manager = newManager("myqueue", testJob, 10)
	assert.Equal(t, 10, manager.concurrency)

	//no per-manager middleware means 'use global Middleware object'", func() {
	manager = newManager("myqueue", testJob, 10)
	assert.Equal(t, Middleware, manager.mids)

	//per-manager middlewares create separate middleware chains", func() {
	mid1 := customMid{Base: "0"}
	manager = newManager("myqueue", testJob, 10, &mid1)
	assert.NotEqual(t, Middleware, manager.mids)
	assert.Equal(t, len(Middleware.actions)+1, len(manager.mids.actions))
}

var message, _ = NewMsg("{\"foo\":\"bar\",\"args\":[\"foo\",\"bar\"]}")
var message2, _ = NewMsg("{\"foo\":\"bar2\",\"args\":[\"foo\",\"bar2\"]}")

func TestQueueProcessing(t *testing.T) {
	namespace := "prod"
	setupTestConfigWithNamespace(namespace)
	rc := Config.Client

	processed := make(chan *Args)
	testJob := (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	manager := newManager("manager1", testJob, 10)

	rc.LPush("prod:queue:manager1", message.ToJson()).Result()
	rc.LPush("prod:queue:manager1", message2.ToJson()).Result()

	manager.start()

	assert.Equal(t, message.Args(), <-processed)
	assert.Equal(t, message2.Args(), <-processed)

	manager.quit()

	len, _ := rc.LLen("prod:queue:manager1").Result()
	assert.Equal(t, int64(0), len)
}

func TestDrainQueueOnExit(t *testing.T) {
	namespace := "prod"
	setupTestConfigWithNamespace(namespace)
	rc := Config.Client

	processed := make(chan *Args)

	sentinel, _ := NewMsg("{\"foo\":\"bar2\",\"args\":\"sentinel\"}")

	drained := false

	slowJob := (func(message *Msg) error {
		if message.ToJson() == sentinel.ToJson() {
			drained = true
		} else {
			processed <- message.Args()
		}

		time.Sleep(1 * time.Second)

		return nil
	})
	manager := newManager("manager1", slowJob, 10)

	for i := 0; i < 9; i++ {
		rc.LPush("prod:queue:manager1", message.ToJson()).Result()
	}
	rc.LPush("prod:queue:manager1", sentinel.ToJson()).Result()

	manager.start()
	for i := 0; i < 9; i++ {
		<-processed
	}
	manager.quit()

	len, _ := rc.LLen("prod:queue:manager1").Result()
	assert.Equal(t, int64(0), len)
	assert.True(t, drained)
}

func TestMultiMiddleware(t *testing.T) {
	//per-manager middlwares are called separately, global middleware is called in each manager", func() {
	namespace := "prod"
	setupTestConfigWithNamespace(namespace)
	rc := Config.Client

	processed := make(chan *Args)

	testJob := (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	mid1 := customMid{Base: "1"}
	mid2 := customMid{Base: "2"}
	mid3 := customMid{Base: "3"}

	oldMiddleware := Middleware
	Middleware = NewMiddleware()
	Middleware.Append(&mid1)

	manager1 := newManager("manager1", testJob, 10)
	manager2 := newManager("manager2", testJob, 10, &mid2)
	manager3 := newManager("manager3", testJob, 10, &mid3)

	rc.LPush("prod:queue:manager1", message.ToJson()).Result()
	rc.LPush("prod:queue:manager2", message.ToJson()).Result()
	rc.LPush("prod:queue:manager3", message.ToJson()).Result()

	manager1.start()
	manager2.start()
	manager3.start()

	<-processed
	<-processed
	<-processed

	Middleware = oldMiddleware

	assert.Equal(t, []string{"11", "12", "11", "12", "11", "12"}, mid1.Trace())
	assert.Equal(t, mid2.Trace(), []string{"21", "22"}, mid2.Trace())
	assert.Equal(t, mid3.Trace(), []string{"31", "32"}, mid3.Trace())

	manager1.quit()
	manager2.quit()
	manager3.quit()
}

func TestStopFetching(t *testing.T) {
	//prepare stops fetching new messages from queue
	namespace := "prodstop:"
	setupTestConfigWithNamespace(namespace)
	rc := Config.Client

	processed := make(chan *Args)
	testJob := (func(message *Msg) error {
		processed <- message.Args()
		return nil
	})

	manager := newManager("manager2", testJob, 10)
	manager.start()

	manager.prepare()

	rc.LPush("prodstop:queue:manager2", message.ToJson()).Result()
	rc.LPush("prodstop:queue:manager2", message2.ToJson()).Result()

	manager.quit()

	len, _ := rc.LLen("prodstop:queue:manager2").Result()
	assert.Equal(t, int64(2), len)
}
