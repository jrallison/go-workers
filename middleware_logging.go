package workers

import (
	"fmt"
	"runtime"
	"time"
)

type MiddlewareLogging struct{}

func (l *MiddlewareLogging) Call(queue string, messages Msgs, next func() bool) (acknowledge bool) {
	start := time.Now()

	for _, m := range messages {
		prefix := fmt.Sprint(queue, " JID-", m.Jid())

		Logger.Println(prefix, "start")
		Logger.Println(prefix, "args: ", m.Args().ToJson())
	}

	defer func() {
		if e := recover(); e != nil {
			for _, m := range messages {
				prefix := fmt.Sprint(queue, " JID-", m.Jid())
				Logger.Println(prefix, "fail:", time.Since(start))
			}

			buf := make([]byte, 4096)
			buf = buf[:runtime.Stack(buf, false)]
			Logger.Printf("error: %v\n%s", e, buf)

			panic(e)
		}
	}()

	acknowledge = next()

	for _, m := range messages {
		prefix := fmt.Sprint(queue, " JID-", m.Jid())
		Logger.Println(prefix, "done:", time.Since(start))
	}

	return
}
