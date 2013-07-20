package workers

import (
	"fmt"
	"time"
)

type MiddlewareLogging struct{}

func (l *MiddlewareLogging) Call(queue string, message *Msg, next func()) {
	prefix := fmt.Sprint(queue, " JID-", message.Jid())

	start := time.Now()
	logger.Println(prefix, "start")
	logger.Println(prefix, "args: ", message.Args().ToJson())

	defer func() {
		if e := recover(); e != nil {
			logger.Println(prefix, "fail:", time.Since(start))
			logger.Println(prefix, "error:", e)
			panic(e)
		}
	}()

	next()

	logger.Println(prefix, "done:", time.Since(start))
}
