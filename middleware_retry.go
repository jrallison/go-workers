package workers

import (
	"time"
)

const (
	DEFAULT_MAX_RETRY = 25
)

type MiddlewareRetry struct{}

func (r *MiddlewareRetry) Call(queue string, message *Msg, next func()) {
	defer func() {
		if e := recover(); e != nil {
			conn := Config.pool.Get()
			defer conn.Close()

			if retry(message) {
				conn.Do("zadd", "goretry", time.Now().Unix(), message.ToJson())
			}

			panic(e)
		}
	}()

	next()
}

func retry(message *Msg) bool {
	retry := false
	max := DEFAULT_MAX_RETRY

	if param, err := message.Get("retry").Bool(); err == nil {
		retry = param
	} else if param, err := message.Get("retry").Int(); err == nil {
		max = param
		retry = true
	}

	count, _ := message.Get("retry_count").Int()

	return retry && count < max
}
