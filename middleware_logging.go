package workers

import (
	"fmt"
	"time"
)

type MiddlewareLogging struct{}

func (l *MiddlewareLogging) Call(queue string, message *Msg, next func()) {
	prefix := fmt.Sprint(queue, " JID-", message.Jid())

	start := time.Now()
	fmt.Println(prefix, "start")
	fmt.Println(prefix, "args: ", message.Args().ToJson())

	defer func() {
		if e := recover(); e != nil {
			fmt.Println(prefix, "fail:", time.Since(start))
			panic(e)
		}
	}()

	next()

	fmt.Println(prefix, "done:", time.Since(start))
}
