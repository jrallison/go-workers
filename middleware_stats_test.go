package workers

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProcessedStats(t *testing.T) {
	namespace := "prod"
	setupTestConfigWithNamespace(namespace)
	rc := Config.Client

	count, _ := rc.Get("prod:stat:processed").Result()
	countInt, _ := strconv.ParseInt(count, 10, 64)
	assert.Equal(t, int64(0), countInt)

	layout := "2006-01-02"
	dayCount, _ := rc.Get("prod:stat:processed:" + time.Now().UTC().Format(layout)).Result()
	dayCountInt, _ := strconv.ParseInt(dayCount, 10, 64)
	assert.Equal(t, int64(0), dayCountInt)

	var job = (func(message *Msg) error {
		// noop
		return nil
	})

	manager := newManager("myqueue", job, 1)
	worker := newWorker(manager)
	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")
	worker.process(message)

	count, _ = rc.Get("prod:stat:processed").Result()
	countInt, _ = strconv.ParseInt(count, 10, 64)
	assert.Equal(t, int64(1), countInt)

	dayCount, _ = rc.Get("prod:stat:processed:" + time.Now().UTC().Format(layout)).Result()
	dayCountInt, _ = strconv.ParseInt(dayCount, 10, 64)
	assert.Equal(t, int64(1), dayCountInt)
}

func TestFailedStats(t *testing.T) {
	layout := "2006-01-02"
	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

	namespace := "prod"
	setupTestConfigWithNamespace(namespace)

	var job = (func(message *Msg) error {
		panic(errors.New("AHHHH"))
	})

	manager := newManager("myqueue", job, 1)
	worker := newWorker(manager)

	rc := Config.Client

	count, _ := rc.Get("prod:stat:failed").Result()
	countInt, _ := strconv.ParseInt(count, 10, 64)
	assert.Equal(t, int64(0), countInt)

	dayCount, _ := rc.Get("prod:stat:failed:" + time.Now().UTC().Format(layout)).Result()
	dayCountInt, _ := strconv.ParseInt(dayCount, 10, 64)
	assert.Equal(t, int64(0), dayCountInt)

	worker.process(message)

	count, _ = rc.Get("prod:stat:failed").Result()
	countInt, _ = strconv.ParseInt(count, 10, 64)
	assert.Equal(t, int64(1), countInt)

	dayCount, _ = rc.Get("prod:stat:failed:" + time.Now().UTC().Format(layout)).Result()
	dayCountInt, _ = strconv.ParseInt(dayCount, 10, 64)
	assert.Equal(t, int64(1), dayCountInt)
}
