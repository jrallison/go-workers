package workers

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	NanoSecondPrecision = 1000000000.0
)

type EnqueueData struct {
	Queue      string      `json:"queue,omitempty"`
	Class      string      `json:"class"`
	Args       interface{} `json:"args"`
	Jid        string      `json:"jid"`
	EnqueuedAt float64     `json:"enqueued_at"`
	EnqueueOptions
}

type EnqueueOptions struct {
	RetryCount int     `json:"retry_count,omitempty"`
	Retry      bool    `json:"retry,omitempty"`
	At         float64 `json:"at,omitempty"`
}

func generateJid() string {
	// Return 12 random bytes as 24 character hex
	b := make([]byte, 12)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}

// Enqueue returns the job ID and queue size (when no errors occur).
func Enqueue(queue, class string, args interface{}) (string, int, error) {
	return EnqueueWithOptions(queue, class, args, EnqueueOptions{At: nowToSecondsWithNanoPrecision()})
}

// EnqueueIn returns the job ID and the size of the scheduled job queue (when no errors occur).
func EnqueueIn(queue, class string, in float64, args interface{}) (string, int, error) {
	return EnqueueWithOptions(queue, class, args, EnqueueOptions{At: nowToSecondsWithNanoPrecision() + in})
}

// EnqueueIn returns the job ID and queue size (when no errors occur). If at is in the future (i.e.
// it’s a scheduled job) the returned queue size is the number of scheduled jobs in the scheduled
// jobs queue.
func EnqueueAt(queue, class string, at time.Time, args interface{}) (string, int, error) {
	return EnqueueWithOptions(queue, class, args, EnqueueOptions{At: timeToSecondsWithNanoPrecision(at)})
}

// EnqueueWithOptions returns the job ID and queue size (when no errors occur). If opts.At is in
// the future (i.e. it’s a scheduled job) the returned queue size is the number of scheduled jobs
// in the scheduled jobs queue.
func EnqueueWithOptions(queue, class string, args interface{}, opts EnqueueOptions) (string, int, error) {
	now := nowToSecondsWithNanoPrecision()
	data := EnqueueData{
		Queue:          queue,
		Class:          class,
		Args:           args,
		Jid:            generateJid(),
		EnqueuedAt:     now,
		EnqueueOptions: opts,
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return "", 0, err
	}

	if now < opts.At {
		err := enqueueAt(data.At, bytes)
		queueSize := 0
		if err == nil {
			queueSize, _ = ScheduledQueueSize()
		}
		return data.Jid, queueSize, err
	}

	conn := Config.Pool.Get()
	defer conn.Close()

	_, err = conn.Do("sadd", Config.Namespace+"queues", queue)
	if err != nil {
		return "", 0, err
	}
	queueSize, err := redis.Int(conn.Do("rpush", queueKey(queue), bytes))
	if err != nil {
		return "", 0, err
	}

	return data.Jid, queueSize, nil
}

func enqueueAt(at float64, bytes []byte) error {
	conn := Config.Pool.Get()
	defer conn.Close()

	_, err := conn.Do("zadd", scheduledQueueKey(), at, bytes)
	return err
}

func timeToSecondsWithNanoPrecision(t time.Time) float64 {
	return float64(t.UnixNano()) / NanoSecondPrecision
}

func durationToSecondsWithNanoPrecision(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / NanoSecondPrecision
}

func nowToSecondsWithNanoPrecision() float64 {
	return timeToSecondsWithNanoPrecision(time.Now())
}
