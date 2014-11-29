package main

import (
	"fmt"
	"time"

	"github.com/Scalingo/go-workers"
)

func main() {
	workers.Configure(map[string]string{
		"server":    "localhost:6379",
		"database":  "0",
		"pool":      "30",
		"process":   "1",
		"namespace": "goworkers",
	})

	workers.Process("myqueue", Task, 10)
	go workers.Run()
	workers.Enqueue("myqueue", "Task", map[string]interface{}{"foo": "bar"})
	workers.EnqueueIn("myqueue", "Task", 2.0, map[string]interface{}{"foo": "bar"})

	time.Sleep(20 * time.Second)
}

func Task(msg *workers.Msg) {
	fmt.Println(msg)
}
