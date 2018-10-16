package workers

import (
	"errors"
	"fmt"
	"time"
)

type MiddlewareStats struct{}

func (l *MiddlewareStats) Call(queue string, message *Msg, next func() error) (err error) {
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
			incrementStats("failed")
		}
	}()

	err = next()
	if err != nil {
		incrementStats("failed")
	}

	incrementStats("processed")

	return
}

func incrementStats(metric string) {
	rc := Config.Client

	today := time.Now().UTC().Format("2006-01-02")

	pipe := rc.Pipeline()
	pipe.Incr(Config.Namespace + "stat:" + metric)
	pipe.Incr(Config.Namespace + "stat:" + metric + ":" + today)

	if _, err := pipe.Exec(); err != nil {
		Logger.Println("couldn't save stats:", err)
	}
}
