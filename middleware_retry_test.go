package workers

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var panicingJob = (func(message *Msg) {
	panic("AHHHH")
})

var wares = NewMiddleware(
	&MiddlewareRetry{},
)

func TestRetryQueue(t *testing.T) {
	setupTestConfigWithNamespace("prod")
	manager := newManager("myqueue", panicingJob, 1)
	worker := newWorker(manager)

	//puts messages in retry queue when they fail
	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

	wares.call("myqueue", message, func() {
		worker.process(message)
	})

	rc := Config.Client

	retries, _ := rc.ZRange("prod:"+RETRY_KEY, 0, 1).Result()
	assert.Equal(t, message.ToJson(), retries[0])
}

func TestDisableRetries(t *testing.T) {
	setupTestConfigWithNamespace("prod")
	manager := newManager("myqueue", panicingJob, 1)
	worker := newWorker(manager)

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":false}")

	wares.call("myqueue", message, func() {
		worker.process(message)
	})

	rc := Config.Client

	count, _ := rc.ZCard("prod:" + RETRY_KEY).Result()
	assert.Equal(t, int64(0), count)
}

func TestNoDefaultRetry(t *testing.T) {
	setupTestConfigWithNamespace("prod")
	manager := newManager("myqueue", panicingJob, 1)
	worker := newWorker(manager)

	message, _ := NewMsg("{\"jid\":\"2\"}")

	wares.call("myqueue", message, func() {
		worker.process(message)
	})

	rc := Config.Client

	count, _ := rc.ZCard("prod:" + RETRY_KEY).Result()
	assert.Equal(t, int64(0), count)
}

func TestNumericRetries(t *testing.T) {
	setupTestConfigWithNamespace("prod")
	manager := newManager("myqueue", panicingJob, 1)
	worker := newWorker(manager)

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":5}")

	wares.call("myqueue", message, func() {
		worker.process(message)
	})

	rc := Config.Client

	retries, _ := rc.ZRange("prod:"+RETRY_KEY, 0, 1).Result()
	assert.Equal(t, message.ToJson(), retries[0])
}

func TestHandleNewFailedMessages(t *testing.T) {
	setupTestConfigWithNamespace("prod")
	manager := newManager("myqueue", panicingJob, 1)
	worker := newWorker(manager)

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

	wares.call("myqueue", message, func() {
		worker.process(message)
	})

	rc := Config.Client

	retries, _ := rc.ZRange("prod:"+RETRY_KEY, 0, 1).Result()
	message, _ = NewMsg(retries[0])

	queue, _ := message.Get("queue").String()
	error_message, _ := message.Get("error_message").String()
	error_class, _ := message.Get("error_class").String()
	retry_count, _ := message.Get("retry_count").Int()
	error_backtrace, _ := message.Get("error_backtrace").String()
	failed_at, _ := message.Get("failed_at").String()

	assert.Equal(t, "prod:myqueue", queue)
	assert.Equal(t, "AHHHH", error_message)
	assert.Equal(t, "", error_class)
	assert.Equal(t, 0, retry_count)
	assert.Equal(t, "", error_backtrace)

	layout := "2006-01-02 15:04:05 MST"
	assert.Equal(t, time.Now().UTC().Format(layout), failed_at)
}

func TestRecurringFailedMessages(t *testing.T) {
	setupTestConfigWithNamespace("prod")

	layout := "2006-01-02 15:04:05 MST"
	manager := newManager("myqueue", panicingJob, 1)
	worker := newWorker(manager)

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true,\"queue\":\"default\",\"error_message\":\"bam\",\"failed_at\":\"2013-07-20 14:03:42 UTC\",\"retry_count\":10}")

	wares.call("myqueue", message, func() {
		worker.process(message)
	})

	rc := Config.Client

	retries, _ := rc.ZRange("prod:"+RETRY_KEY, 0, 1).Result()
	message, _ = NewMsg(retries[0])

	queue, _ := message.Get("queue").String()
	error_message, _ := message.Get("error_message").String()
	retry_count, _ := message.Get("retry_count").Int()
	failed_at, _ := message.Get("failed_at").String()
	retried_at, _ := message.Get("retried_at").String()

	assert.Equal(t, "prod:myqueue", queue)
	assert.Equal(t, "AHHHH", error_message)
	assert.Equal(t, 11, retry_count)
	assert.Equal(t, "2013-07-20 14:03:42 UTC", failed_at)
	assert.Equal(t, time.Now().UTC().Format(layout), retried_at)
}

func TestRecurringFailedMessagesWithMax(t *testing.T) {
	setupTestConfigWithNamespace("prod")

	layout := "2006-01-02 15:04:05 MST"
	manager := newManager("myqueue", panicingJob, 1)
	worker := newWorker(manager)

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":10,\"queue\":\"default\",\"error_message\":\"bam\",\"failed_at\":\"2013-07-20 14:03:42 UTC\",\"retry_count\":8}")

	wares.call("myqueue", message, func() {
		worker.process(message)
	})

	rc := Config.Client

	retries, _ := rc.ZRange("prod:"+RETRY_KEY, 0, 1).Result()
	message, _ = NewMsg(retries[0])

	queue, _ := message.Get("queue").String()
	error_message, _ := message.Get("error_message").String()
	retry_count, _ := message.Get("retry_count").Int()
	failed_at, _ := message.Get("failed_at").String()
	retried_at, _ := message.Get("retried_at").String()

	assert.Equal(t, "prod:myqueue", queue)
	assert.Equal(t, "AHHHH", error_message)
	assert.Equal(t, 9, retry_count)
	assert.Equal(t, "2013-07-20 14:03:42 UTC", failed_at)
	assert.Equal(t, time.Now().UTC().Format(layout), retried_at)
}

func TestRetryOnlyToMax(t *testing.T) {
	setupTestConfigWithNamespace("prod")

	manager := newManager("myqueue", panicingJob, 1)
	worker := newWorker(manager)

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true,\"retry_count\":25}")

	wares.call("myqueue", message, func() {
		worker.process(message)
	})

	rc := Config.Client

	count, _ := rc.ZCard("prod:" + RETRY_KEY).Result()
	assert.Equal(t, int64(0), count)
}

func TestRetryOnlyToCustomMax(t *testing.T) {
	setupTestConfigWithNamespace("prod")

	manager := newManager("myqueue", panicingJob, 1)
	worker := newWorker(manager)

	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":3,\"retry_count\":3}")

	wares.call("myqueue", message, func() {
		worker.process(message)
	})

	rc := Config.Client

	count, _ := rc.ZCard("prod:" + RETRY_KEY).Result()
	assert.Equal(t, int64(0), count)
}
