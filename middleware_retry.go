package workers

import (
	"encoding/json"
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

func (r *MiddlewareRetry) Call(queue string, message *Msg, next func() bool) (acknowledge bool) {
	defer func() {
		if e := recover(); e != nil {
			conn := Config.Pool.Get()
			defer conn.Close()
			if retry(message) {
				message.Set("queue", queue)
				message.Set("error_message", fmt.Sprintf("%v", e))
				retryCount := incrementRetry(message)

				retryOptions, _ := message.Get("retry_options").Map()
				waitDuration := durationToSecondsWithNanoPrecision(
					time.Duration(
						secondsToDelay(retryCount, retryOptions),
					) * time.Second,
				)

				_, err := conn.Do(
					"zadd",
					Config.Namespace+RETRY_KEY,
					nowToSecondsWithNanoPrecision()+waitDuration,
					message.ToJson(),
				)

				// If we can't add the job to the retry queue,
				// then we shouldn't acknowledge the job, otherwise
				// it'll disappear into the void.
				if err != nil {
					acknowledge = false
				}
			}

			panic(e)
		}
	}()

	acknowledge = next()

	return
}

func retry(message *Msg) bool {
	retry := false
	max := DEFAULT_MAX_RETRY

	if param, err := message.Get("retry").Bool(); err == nil {
		retry = param
	} else if param, err := message.Get("retry").Int(); err == nil { // compatible with sidekiq
		max = param
		retry = true
	}

	if param, err := message.Get("retry_max").Int(); err == nil {
		max = param
	}

	count, _ := message.Get("retry_count").Int()

	return retry && count < max
}

func incrementRetry(message *Msg) (retryCount int) {
	retryCount = 0

	if count, err := message.Get("retry_count").Int(); err != nil {
		message.Set("failed_at", time.Now().UTC().Format(LAYOUT))
	} else {
		message.Set("retried_at", time.Now().UTC().Format(LAYOUT))
		retryCount = count + 1
	}

	message.Set("retry_count", retryCount)

	return
}

func secondsToDelay(count int, retryOptions map[string]interface{}) int {
	exp := float64(4)
	minDelay := float64(15)
	maxDelay := math.Inf(1)
	maxRand := float64(30)
	if retryOptions != nil {
		if v, ok := retryOptions["exp"].(json.Number); ok {
			if v2, err := v.Float64(); err == nil {
				exp = v2
			}
		}
		if v, ok := retryOptions["min_delay"].(json.Number); ok {
			if v2, err := v.Float64(); err == nil {
				minDelay = v2
			}
		}
		if v, ok := retryOptions["max_delay"].(json.Number); ok {
			if v2, err := v.Float64(); err == nil {
				maxDelay = v2
			}
		}
		if v, ok := retryOptions["max_rand"].(json.Number); ok {
			if v2, err := v.Float64(); err == nil {
				maxRand = v2
			}
		}
	}

	power := math.Pow(float64(count), exp)
	randN := 0
	if maxRand > 0 {
		randN = rand.Intn(int(maxRand))
	}
	return int(math.Min(power+minDelay+float64(randN*(count+1)), maxDelay))
}
