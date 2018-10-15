package workers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var recoverOnPanic = func(f func()) (err error) {
	defer func() {
		if cause := recover(); cause != nil {
			var ok bool
			err, ok = cause.(error)
			if !ok {
				err = fmt.Errorf("not error; %v", cause)
			}
		}
	}()

	f()

	return
}

func TestRedisPoolConfig(t *testing.T) {
	// Tests redis pool size which defaults to 1
	Configure(map[string]string{
		"server":  "localhost:6379",
		"process": "2",
	})
	assert.Equal(t, 1, Config.Client.Options().PoolSize)

	Configure(map[string]string{
		"server":  "localhost:6379",
		"process": "1",
		"pool":    "20",
	})

	assert.Equal(t, 20, Config.Client.Options().PoolSize)
}

func TestCustomProcessConfig(t *testing.T) {
	Configure(map[string]string{
		"server":  "localhost:6379",
		"process": "1",
	})
	assert.Equal(t, "1", Config.processId)

	Configure(map[string]string{
		"server":  "localhost:6379",
		"process": "2",
	})
	assert.Equal(t, "2", Config.processId)
}

func TestRequiresRedisConfig(t *testing.T) {
	err := recoverOnPanic(func() {
		Configure(map[string]string{"process": "2"})
	})

	assert.Error(t, err, "Configure requires a 'server' or 'sentinels' options, which identify either Redis instance or sentinels.")
}

func TestRequiresProcessConfig(t *testing.T) {
	err := recoverOnPanic(func() {
		Configure(map[string]string{"server": "localhost:6379"})
	})

	assert.Error(t, err, "Configure requires a 'process' option, which uniquely identifies this instance")
}

func TestAddsColonToNamespace(t *testing.T) {
	Configure(map[string]string{
		"server":  "localhost:6379",
		"process": "1",
	})
	assert.Equal(t, "", Config.Namespace)

	Configure(map[string]string{
		"server":    "localhost:6379",
		"process":   "1",
		"namespace": "prod",
	})
	assert.Equal(t, "prod:", Config.Namespace)
}

func TestDefaultPoolIntervalConfig(t *testing.T) {
	Configure(map[string]string{
		"server":  "localhost:6379",
		"process": "1",
	})

	assert.Equal(t, 15, Config.PollInterval)

	Configure(map[string]string{
		"server":        "localhost:6379",
		"process":       "1",
		"poll_interval": "1",
	})

	assert.Equal(t, 1, Config.PollInterval)
}

func TestSentinelConfig(t *testing.T) {
	Configure(map[string]string{
		"sentinels":     "localhost:26379,localhost:46379",
		"process":       "1",
		"poll_interval": "1",
	})

	assert.Equal(t, "FailoverClient", Config.Client.Options().Addr)
}
