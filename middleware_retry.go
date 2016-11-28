package workers

import (
	"fmt"
	"math"
	"math/rand"
	"time"
)

const (
	DEFAULT_MAX_RETRY = 25
	LAYOUT            = "2006-01-02 15:04:05 MST"
)

type MiddlewareRetry struct{}

func (r *MiddlewareRetry) Call(queue string, messages Msgs, next func() bool) (acknowledge bool) {
	defer func() {
		if e := recover(); e != nil {
			conn := Config.Pool.Get()
			defer conn.Close()

			for _, m := range messages {
				if retry(m) {
					m.queue = queue
					m.error = fmt.Sprintf("%v", e)
					retryCount := incrementRetry(m)

					waitDuration := durationToSecondsWithNanoPrecision(
						time.Duration(
							secondsToDelay(retryCount),
						) * time.Second,
					)

					_, err := conn.Do(
						"zadd",
						Config.Namespace+RETRY_KEY,
						nowToSecondsWithNanoPrecision()+waitDuration,
						m.ToJson(),
					)

					// If we can't add the job to the retry queue,
					// then we shouldn't acknowledge the job, otherwise
					// it'll disappear into the void.
					if err != nil {
						acknowledge = false
					}
				}
			}

			panic(e)
		}
	}()

	acknowledge = next()

	return
}

func retry(message *Msg) bool {
	max := DEFAULT_MAX_RETRY
	retry := message.Retry
	count := message.retryCount
	if message.RetryMax != 0 {
		max = message.RetryMax

	}
	return retry && count < max
}

func incrementRetry(message *Msg) (retryCount int) {
	t := time.Now().UTC().Format(LAYOUT)
	if message.retryCount == -1 {
		message.failedAt = t
	} else {
		message.retriedAt = t
	}
	message.retryCount++

	return message.retryCount
}

func secondsToDelay(count int) int {
	power := math.Pow(float64(count), 4)
	return int(power) + 15 + (rand.Intn(30) * (count + 1))
}
