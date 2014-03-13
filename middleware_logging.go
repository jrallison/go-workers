package workers

import (
	"fmt"
	"time"
)

type MiddlewareLogging struct{}

func (l *MiddlewareLogging) Call(queue string, message *Msg, next func() bool) (acknowledge bool) {
	prefix := fmt.Sprint(queue, " JID-", message.Jid())

	start := time.Now()
	Logger.Println(prefix, "start")
	Logger.Println(prefix, "args: ", message.Args().ToJson())

	defer func() {
		if e := recover(); e != nil {
			Logger.Println(prefix, "fail:", time.Since(start))
			Logger.Println(prefix, "error:", e)
			panic(e)
		}
	}()

	acknowledge = next()

	Logger.Println(prefix, "done:", time.Since(start))

	return
}
