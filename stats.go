package workers

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func Stats(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

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

	stats := map[string]interface{}{
		"processed": 0,
		"failed":    0,
		"jobs":      jobs,
	}

	body, _ := json.MarshalIndent(stats, "", "  ")
	fmt.Fprintln(w, string(body))
}
