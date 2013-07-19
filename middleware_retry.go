package workers

import (
	"time"
)

type MiddlewareRetry struct{}

func (r *MiddlewareRetry) Call(queue string, message *Msg, next func()) {
	defer func() {
		if e := recover(); e != nil {
			conn := Config.pool.Get()
			defer conn.Close()

			if message.Retry() {
				conn.Do("zadd", "goretry", time.Now().Unix(), message.ToJson())
			}

			panic(e)
		}
	}()

	next()
}
