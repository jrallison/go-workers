package workers

import (
	"testing"

	"github.com/go-redis/redis"
	"github.com/stretchr/testify/assert"
)

func TestScheduled(t *testing.T) {
	namespace := "prod"
	setupTestConfigWithNamespace(namespace)
	scheduled := newScheduled(RETRY_KEY)

	rc := Config.Client

	now := nowToSecondsWithNanoPrecision()

	message1, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar1\"}")
	message2, _ := NewMsg("{\"queue\":\"myqueue\",\"foo\":\"bar2\"}")
	message3, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar3\"}")

	rc.ZAdd("prod:"+RETRY_KEY, redis.Z{Score: now - 60.0, Member: message1.ToJson()}).Result()
	rc.ZAdd("prod:"+RETRY_KEY, redis.Z{Score: now - 10.0, Member: message2.ToJson()}).Result()
	rc.ZAdd("prod:"+RETRY_KEY, redis.Z{Score: now + 60.0, Member: message3.ToJson()}).Result()

	scheduled.poll()

	defaultCount, _ := rc.LLen("prod:queue:default").Result()
	myqueueCount, _ := rc.LLen("prod:queue:myqueue").Result()
	pending, _ := rc.ZCard("prod:" + RETRY_KEY).Result()

	assert.Equal(t, int64(1), defaultCount)
	assert.Equal(t, int64(1), myqueueCount)
	assert.Equal(t, int64(1), pending)
}
