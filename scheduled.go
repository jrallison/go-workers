package workers

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

const (
	POLL_INTERVAL = 15
)

type scheduled struct {
	keys   []string
	closed bool
	exit   chan bool
}

func (s *scheduled) start() {
	go s.poll(true)
}

func (s *scheduled) quit() {
	s.closed = true
}

func (s *scheduled) poll(continuing bool) {
	if s.closed {
		return
	}

	conn := Config.Pool.Get()

	now := time.Now().Unix()

	for _, key := range s.keys {
		key = Config.namespace + key
		for {
			messages, _ := redis.Strings(conn.Do("zrangebyscore", key, "-inf", now, "limit", 0, 1))

			if len(messages) == 0 {
				break
			}

			message, _ := NewMsg(messages[0])

			if removed, _ := redis.Bool(conn.Do("zrem", key, messages[0])); removed {
				queue, _ := message.Get("queue").String()
				conn.Do("lpush", Config.namespace+"queue:"+queue, message.ToJson())
			}
		}
	}

	conn.Close()
	if continuing {
		time.Sleep(POLL_INTERVAL * time.Second)
		s.poll(true)
	}
}

func newScheduled(keys ...string) *scheduled {
	return &scheduled{keys, false, make(chan bool)}
}
