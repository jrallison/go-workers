package workers

import (
	"time"
)

type MiddlewareStats struct{}

func (l *MiddlewareStats) Call(queue string, message *Msg, next func() bool) (acknowledge bool) {
	defer func() {
		if e := recover(); e != nil {
			incrementStats("failed")
			panic(e)
		}
	}()

	acknowledge = next()

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
