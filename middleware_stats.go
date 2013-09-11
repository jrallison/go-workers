package workers

import (
	"time"
)

type MiddlewareStats struct{}

func (l *MiddlewareStats) Call(queue string, message *Msg, next func()) {
	defer func() {
		if e := recover(); e != nil {
			incrementStats("failed")
			panic(e)
		}
	}()

	next()

	incrementStats("processed")
}

func incrementStats(metric string) {
	conn := Config.Pool.Get()
	defer conn.Close()

	today := time.Now().UTC().Format("2006-01-02")

	conn.Send("multi")
	conn.Send("incr", Config.namespace+"stat:"+metric)
	conn.Send("incr", Config.namespace+"stat:"+metric+":"+today)

	if _, err := conn.Do("exec"); err != nil {
		Logger.Println("couldn't save stats:", err)
	}
}
