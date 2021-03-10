package workers

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

type scheduled struct {
	keys   []string
	closed chan bool
	exit   chan bool
}

func (s *scheduled) start() {
	go (func() {
		for {
			select {
			case <-s.closed:
				return
			default:
			}

			s.poll()

			time.Sleep(time.Duration(Config.PollInterval) * time.Second)
		}
	})()
}

func (s *scheduled) quit() {
	close(s.closed)
}

func (s *scheduled) poll() {
	now := nowToSecondsWithNanoPrecision()

	for _, key := range s.keys {
		key = Config.Namespace + key
		for {
			messages, _ := Config.Redis.ZRangeByScore(context.Background(), key, &redis.ZRangeBy{
				Min:    "-inf",
				Max:    strconv.FormatFloat(now, 'f', 0, 64),
				Offset: 0,
				Count:  1,
			}).Result()

			if len(messages) == 0 {
				break
			}

			message, _ := NewMsg(messages[0])

			if removed, _ := Config.Redis.ZRem(context.Background(), key, messages[0]).Result(); removed != 0 {
				queue, _ := message.Get("queue").String()
				queue = strings.TrimPrefix(queue, Config.Namespace)
				message.Set("enqueued_at", nowToSecondsWithNanoPrecision())
				Config.Redis.LPush(context.Background(), Config.Namespace+"queue:"+queue, message.ToJson())
			}
		}
	}
}

func newScheduled(keys ...string) *scheduled {
	return &scheduled{keys, make(chan bool), make(chan bool)}
}
