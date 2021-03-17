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
	today := time.Now().UTC().Format("2006-01-02")

	p := Config.Redis.Pipeline()
	p.Incr(Config.Namespace + "stat:" + metric)
	p.Incr(Config.Namespace + "stat:" + metric + ":" + today)

	if _, err := p.Exec(); err != nil {
		Logger.Println("couldn't save stats:", err)
	}
}
