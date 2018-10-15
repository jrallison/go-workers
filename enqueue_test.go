package workers

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnqueue(t *testing.T) {
	namespace := "prod"
	setupTestConfigWithNamespace(namespace)
	rc := Config.Client

	//makes the queue available
	Enqueue("enqueue1", "Add", []int{1, 2})

	found, _ := rc.SIsMember("prod:queues", "enqueue1").Result()
	assert.True(t, found)

	// adds a job to the queue
	nb, _ := rc.LLen("prod:queue:enqueue2").Result()
	assert.Equal(t, int64(0), nb)

	Enqueue("enqueue2", "Add", []int{1, 2})

	nb, _ = rc.LLen("prod:queue:enqueue2").Result()
	assert.Equal(t, int64(1), nb)

	//saves the arguments
	Enqueue("enqueue3", "Compare", []string{"foo", "bar"})

	bytes, _ := rc.LPop("prod:queue:enqueue3").Result()
	var result map[string]interface{}
	json.Unmarshal([]byte(bytes), &result)
	assert.Equal(t, "Compare", result["class"])

	args := result["args"].([]interface{})
	assert.Equal(t, 2, len(args))
	assert.Equal(t, "foo", args[0])
	assert.Equal(t, "bar", args[1])

	//has a jid
	Enqueue("enqueue4", "Compare", []string{"foo", "bar"})

	bytes, _ = rc.LPop("prod:queue:enqueue4").Result()
	json.Unmarshal([]byte(bytes), &result)
	assert.Equal(t, "Compare", result["class"])

	jid := result["jid"].(string)
	assert.Equal(t, 24, len(jid))

	//has enqueued_at that is close to now
	Enqueue("enqueue5", "Compare", []string{"foo", "bar"})

	bytes, _ = rc.LPop("prod:queue:enqueue5").Result()
	json.Unmarshal([]byte(bytes), &result)
	assert.Equal(t, "Compare", result["class"])

	ea := result["enqueued_at"].(float64)
	assert.InDelta(t, nowToSecondsWithNanoPrecision(), ea, 0.1)

	// has retry and retry_count when set
	EnqueueWithOptions("enqueue6", "Compare", []string{"foo", "bar"}, EnqueueOptions{RetryCount: 13, Retry: true})

	bytes, _ = rc.LPop("prod:queue:enqueue6").Result()
	json.Unmarshal([]byte(bytes), &result)
	assert.Equal(t, "Compare", result["class"])

	retry := result["retry"].(bool)
	assert.True(t, retry)

	retryCount := int(result["retry_count"].(float64))
	assert.Equal(t, 13, retryCount)
}

func TestEnqueueSpec(t *testing.T) {
	namespace := "prod"
	setupTestConfigWithNamespace(namespace)
	rc := Config.Client

	scheduleQueue := namespace + ":" + SCHEDULED_JOBS_KEY

	//has added a job in the scheduled queue
	_, err := EnqueueIn("enqueuein1", "Compare", 10, map[string]interface{}{"foo": "bar"})
	assert.Nil(t, err)

	scheduledCount, _ := rc.ZCard(scheduleQueue).Result()
	assert.Equal(t, int64(1), scheduledCount)

	rc.Del(scheduleQueue)

	//has the correct 'queue'
	_, err = EnqueueIn("enqueuein2", "Compare", 10, map[string]interface{}{"foo": "bar"})
	assert.Nil(t, err)

	var data EnqueueData
	elem, err := rc.ZRange(scheduleQueue, 0, -1).Result()
	bytes := elem[0]
	json.Unmarshal([]byte(bytes), &data)

	assert.Equal(t, "enqueuein2", data.Queue)

	rc.Del(scheduleQueue)
}

func TestMultipleEnqueueOrder(t *testing.T) {
	namespace := "prod"
	setupTestConfigWithNamespace(namespace)
	rc := Config.Client

	var msg1, _ = NewMsg("{\"key\":\"1\"}")
	_, err := Enqueue("testq1", "Compare", msg1.ToJson())
	assert.Nil(t, err)

	var msg2, _ = NewMsg("{\"key\":\"2\"}")
	_, err = Enqueue("testq1", "Compare", msg2.ToJson())
	assert.Nil(t, err)

	len, _ := rc.LLen("prod:queue:testq1").Result()
	assert.Equal(t, int64(2), len)

	bytesMsg, err := rc.RPop("prod:queue:testq1").Result()
	assert.Nil(t, err)
	var data EnqueueData
	json.Unmarshal([]byte(bytesMsg), &data)
	actualMsg, err := NewMsg(data.Args.(string))
	assert.Equal(t, msg1.Get("key"), actualMsg.Get("key"))

	bytesMsg, err = rc.RPop("prod:queue:testq1").Result()
	assert.Nil(t, err)
	json.Unmarshal([]byte(bytesMsg), &data)
	actualMsg, err = NewMsg(data.Args.(string))
	assert.Equal(t, msg2.Get("key"), actualMsg.Get("key"))

	len, _ = rc.LLen("prod:queue:testq1").Result()
	assert.Equal(t, int64(0), len)
}
