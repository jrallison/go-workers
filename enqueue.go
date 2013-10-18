package workers

import (
	"encoding/json"
)

type EnqueueData struct {
	Class string `json:"class"`
	Args  interface{} `json:"args"`
}

func Enqueue(queue, class string, args interface{}) error {
	conn := Config.Pool.Get()
	defer conn.Close()

	bytes, err := json.Marshal(EnqueueData{class, args})
	if err != nil {
		return err
	}

	_, err = conn.Do("sadd", Config.namespace + "queues", queue)
	if err != nil {
		return err
	}
	queue = Config.namespace + "queue:" + queue
	_, err = conn.Do("rpush", queue, bytes)
	return err
}
