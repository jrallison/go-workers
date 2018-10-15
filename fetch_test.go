package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func buildFetch(queue string) Fetcher {
	manager := newManager(queue, nil, 1)
	fetch := manager.fetch
	go fetch.Fetch()
	return fetch
}

func TestFetchConfig(t *testing.T) {
	setupTestConfig()
	fetch := buildFetch("fetchQueue1")
	assert.Equal(t, "queue:fetchQueue1", fetch.Queue())
	fetch.Close()
}

func TestGetMessagesToChannel(t *testing.T) {
	setupTestConfig()

	message, _ := NewMsg("{\"foo\":\"bar\"}")
	fetch := buildFetch("fetchQueue2")

	rc := Config.Client

	rc.LPush("queue:fetchQueue2", message.ToJson()).Result()

	fetch.Ready() <- true
	fetchedMessage := <-fetch.Messages()

	assert.Equal(t, message, fetchedMessage)

	len, _ := rc.LLen("queue:fetchQueue2").Result()
	assert.Equal(t, int64(0), len)

	fetch.Close()
}

func TestMoveProgressMessageToPrivateQueue(t *testing.T) {
	setupTestConfig()
	message, _ := NewMsg("{\"foo\":\"bar\"}")

	fetch := buildFetch("fetchQueue3")

	rc := Config.Client

	rc.LPush("queue:fetchQueue3", message.ToJson())

	fetch.Ready() <- true
	<-fetch.Messages()

	len, _ := rc.LLen("queue:fetchQueue3:1:inprogress").Result()
	assert.Equal(t, int64(1), len)

	messages, _ := rc.LRange("queue:fetchQueue3:1:inprogress", 0, -1).Result()
	assert.Equal(t, message.ToJson(), messages[0])

	fetch.Close()
}

func TestRemoveProgressMessageWhenAcked(t *testing.T) {
	setupTestConfig()
	message, _ := NewMsg("{\"foo\":\"bar\"}")

	fetch := buildFetch("fetchQueue4")

	rc := Config.Client

	rc.LPush("queue:fetchQueue4", message.ToJson()).Result()

	fetch.Ready() <- true
	<-fetch.Messages()

	fetch.Acknowledge(message)

	len, _ := rc.LLen("queue:fetchQueue4:1:inprogress").Result()
	assert.Equal(t, int64(0), len)

	fetch.Close()
}

func TestRemoveProgressMessageDifferentSerialization(t *testing.T) {
	setupTestConfig()
	json := "{\"foo\":\"bar\",\"args\":[]}"
	message, _ := NewMsg(json)

	assert.NotEqual(t, message.ToJson(), json)

	fetch := buildFetch("fetchQueue5")

	rc := Config.Client

	rc.LPush("queue:fetchQueue5", json).Result()

	fetch.Ready() <- true
	<-fetch.Messages()

	fetch.Acknowledge(message)

	len, _ := rc.LLen("queue:fetchQueue5:1:inprogress").Result()
	assert.Equal(t, int64(0), len)

	fetch.Close()
}

func TestRetryInprogressMessages(t *testing.T) {
	setupTestConfig()
	message, _ := NewMsg("{\"foo\":\"bar\"}")
	message2, _ := NewMsg("{\"foo\":\"bar2\"}")
	message3, _ := NewMsg("{\"foo\":\"bar3\"}")

	rc := Config.Client

	rc.LPush("queue:fetchQueue6:1:inprogress", message.ToJson()).Result()
	rc.LPush("queue:fetchQueue6:1:inprogress", message2.ToJson()).Result()
	rc.LPush("queue:fetchQueue6", message3.ToJson()).Result()

	fetch := buildFetch("fetchQueue6")

	fetch.Ready() <- true
	assert.Equal(t, message2, <-fetch.Messages())
	fetch.Ready() <- true
	assert.Equal(t, message, <-fetch.Messages())
	fetch.Ready() <- true
	assert.Equal(t, message3, <-fetch.Messages())

	fetch.Acknowledge(message)
	fetch.Acknowledge(message2)
	fetch.Acknowledge(message3)

	len, _ := rc.LLen("queue:fetchQueue6:1:inprogress").Result()
	assert.Equal(t, int64(0), len)

	fetch.Close()
}
