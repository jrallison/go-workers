package workers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)

type stats struct {
	Processed int         `json:"processed"`
	Failed    int         `json:"failed"`
	Jobs      interface{} `json:"jobs"`
}

func Stats(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	jobs := make(map[string][]*map[string]interface{})

	for _, m := range managers {
		queue := m.queueName()
		jobs[queue] = make([]*map[string]interface{}, 0)

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

	stats := stats{
		0,
		0,
		jobs,
	}

	conn := Config.Pool.Get()
	defer conn.Close()

	conn.Send("multi")
	conn.Send("get", Config.Namespace+"stat:processed")
	conn.Send("get", Config.Namespace+"stat:failed")
	r, err := conn.Do("exec")

	if err != nil {
		Logger.Println("couldn't retrieve stats:", err)
	}

	results := r.([]interface{})

	if len(results) == 2 {
		if results[0] != nil {
			stats.Processed, _ = strconv.Atoi(string(results[0].([]byte)))
		}
		if results[1] != nil {
			stats.Failed, _ = strconv.Atoi(string(results[1].([]byte)))
		}
	}

	body, _ := json.MarshalIndent(stats, "", "  ")
	fmt.Fprintln(w, string(body))
}
