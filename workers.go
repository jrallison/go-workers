package workers

import (
	"fmt"
	"log"
	"net/http"
	"os"
)

const (
	RETRY_KEY          = "goretry"
	SCHEDULED_JOBS_KEY = "schedule"
)

var Logger WorkersLogger = log.New(os.Stdout, "workers: ", log.Ldate|log.Lmicroseconds)

var managers = make(map[string]*manager)
var schedule = newScheduled(RETRY_KEY, SCHEDULED_JOBS_KEY)
var control = make(map[string]chan string)

var Middleware = NewMiddleware(
	&MiddlewareLogging{},
	&MiddlewareRetry{},
	&MiddlewareStats{},
)

func Process(queue string, job jobFunc, concurrency int, mids ...Action) {
	managers[queue] = newManager(queue, job, concurrency, mids...)
}

func Run() {
	Start()
	go handleSignals()
	waitForExit()
}

func Start() {
	schedule.start()
	startManagers()
}

func Quit() {
	quitManagers()
	schedule.quit()
	waitForExit()
}

func StatsServer(port int) {
	http.HandleFunc("/stats", Stats)

	Logger.Println("Stats are available at", fmt.Sprint("http://localhost:", port, "/stats"))

	if err := http.ListenAndServe(fmt.Sprint(":", port), nil); err != nil {
		Logger.Println(err)
	}
}

func startManagers() {
	for _, manager := range managers {
		manager.start()
	}
}

func quitManagers() {
	for _, m := range managers {
		go (func(m *manager) { m.quit() })(m)
	}
}

func waitForExit() {
	for queue, manager := range managers {
		manager.Wait()
		delete(managers, queue)
	}
}
