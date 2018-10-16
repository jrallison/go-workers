package workers

import (
	"errors"
	"fmt"
	"runtime"
	"time"
)

type MiddlewareLogging struct{}

func (l *MiddlewareLogging) procesError(prefix string, start time.Time, err error) {
	Logger.Println(prefix, "fail:", time.Since(start))

	buf := make([]byte, 4096)
	buf = buf[:runtime.Stack(buf, false)]
	Logger.Printf("%s error: %v\n%s", prefix, err, buf)
}

func (l *MiddlewareLogging) Call(queue string, message *Msg, next func() error) (err error) {
	prefix := fmt.Sprint(queue, " JID-", message.Jid())

	start := time.Now()
	Logger.Println(prefix, "start")
	Logger.Println(prefix, "args:", message.Args().ToJson())

	defer func() {
		if e := recover(); e != nil {
			lerr, ok := e.(error)
			if !ok {
				err = errors.New(fmt.Sprintf("Unable to get error from recover(): %v", e))
			} else {
				err = lerr
			}
		}

		if err != nil {
			l.procesError(prefix, start, err)
		}
	}()

	err = next()
	if err != nil {
		l.procesError(prefix, start, err)
	}

	Logger.Println(prefix, "done:", time.Since(start))

	return
}
