package workers

import (
	"log"
	"os"
)

var managers = make(map[string]*manager)
var control = make(map[string]chan string)
var logger = log.New(os.Stdout, "background: ", log.Ldate|log.Lmicroseconds)

var Middleware = newMiddleware(
	&MiddlewareLogging{},
	&MiddlewareRetry{},
)

func Process(queue string, job jobFunc, concurrency int) {
	managers[queue] = newManager(queue, job, concurrency)
	managers[queue].start()
}

func Run() {
	scheduled := newScheduled("goretry")
	scheduled.start()

	for _, manager := range managers {
		manager.Wait()
	}

	scheduled.quit()
}
