package workers

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

const (
	RETRY_KEY = "goretry"
)

var Logger = log.New(os.Stdout, "workers: ", log.Ldate|log.Lmicroseconds)

var managers = make(map[string]*manager)
var schedule = newScheduled(RETRY_KEY)
var control = make(map[string]chan string)

var Middleware = newMiddleware(
	&MiddlewareLogging{},
	&MiddlewareRetry{},
	&MiddlewareStats{},
)

func Process(queue string, job jobFunc, concurrency int) {
	managers[queue] = newManager(queue, job, concurrency)
}

func Run() {
	schedule.start()
	startManagers()
	go handleSignals()
	waitForExit()
}

func Quit() {
	quitManagers()
	schedule.quit()
}

func StatsServer(port int) {
	http.HandleFunc("/stats", Stats)

	Logger.Println("Stats are available at", fmt.Sprint("http://localhost:", port, "/stats"))

	if err := http.ListenAndServe(fmt.Sprint(":", port), nil); err != nil {
		Logger.Println(err)
	}
}

func handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR1, syscall.SIGINT, syscall.SIGTERM)

	for sig := range signals {
		switch sig {
		case syscall.SIGINT, syscall.SIGUSR1, syscall.SIGTERM:
			Quit()
		}
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
	for _, manager := range managers {
		manager.Wait()
	}
}
