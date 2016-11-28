package workers

import (
	"time"
)

type MiddlewareStats struct{}

func (l *MiddlewareStats) Call(queue string, messages Msgs, next func() bool) (acknowledge bool) {
	defer func() {
		if e := recover(); e != nil {
			incrementStats("failed", len(messages))
			panic(e)
		}
	}()

	acknowledge = next()

	incrementStats("processed", len(messages))

	return
}

func incrementStats(metric string, count int) {
	conn := Config.Pool.Get()
	defer conn.Close()

	today := time.Now().UTC().Format("2006-01-02")

	conn.Send("multi")
	conn.Send("incrby", Config.Namespace+"stat:"+metric, count)
	conn.Send("incrby", Config.Namespace+"stat:"+metric+":"+today, count)

	if _, err := conn.Do("exec"); err != nil {
		Logger.Println("couldn't save stats:", err)
	}
}
