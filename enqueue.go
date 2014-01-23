package workers

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"time"
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

func Enqueue(queue, class string, args interface{}) error {
	return EnqueueWithOptions(queue, class, args, EnqueueOptions{})
}

func EnqueueWithOptions(queue, class string, args interface{}, opts EnqueueOptions) error {
	conn := Config.Pool.Get()
	defer conn.Close()

	data := EnqueueData{
		Queue:          queue,
		Class:          class,
		Args:           args,
		Jid:            generateJid(),
		EnqueuedAt:     float64(time.Now().UnixNano()) / 1000000000,
		EnqueueOptions: opts,
	}
	bytes, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = conn.Do("sadd", Config.namespace+"queues", queue)
	if err != nil {
		return err
	}
	queue = Config.namespace + "queue:" + queue
	_, err = conn.Do("rpush", queue, bytes)
	return err
}
