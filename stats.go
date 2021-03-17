package workers

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type stats struct {
	Processed int         `json:"processed"`
	Failed    int         `json:"failed"`
	Jobs      interface{} `json:"jobs"`
	Enqueued  interface{} `json:"enqueued"`
	Retries   int64       `json:"retries"`
}

func Stats(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	jobs := make(map[string][]*map[string]interface{})
	enqueued := make(map[string]string)

	stats := stats{
		0,
		0,
		nil,
		nil,
		0,
	}

	for _, m := range managers {
		queue := m.queueName()
		jobs[queue] = make([]*map[string]interface{}, 0)
		enqueued[queue] = ""
		for _, worker := range m.workers {
			message := worker.currentMsg
			startedAt := worker.startedAt

			if message != nil && startedAt > 0 {
				jobs[queue] = append(jobs[queue], &map[string]interface{}{
					"message":    message,
					"started_at": startedAt,
				})
			}
		}
	}
	stats.Jobs = jobs

	stats.Processed, _ = Config.Redis.Get(Config.Namespace + "stat:processed").Int()
	stats.Failed, _ = Config.Redis.Get(Config.Namespace + "stat:failed").Int()
	stats.Retries = Config.Redis.ZCard(Config.Namespace + RETRY_KEY).Val()
	for key := range enqueued {
		enqueued[key] = Config.Redis.LLen(fmt.Sprintf("%squeue:%s", Config.Namespace, key)).String()
	}
	stats.Enqueued = enqueued

	body, _ := json.MarshalIndent(stats, "", "  ")
	fmt.Fprintln(w, string(body))
}
