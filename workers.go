package workers

import (
	"log"
	"os"
	"os/signal"
	"syscall"
)

const (
	RETRY_KEY = "goretry"
)

var managers = make(map[string]*manager)
var schedule = newScheduled(RETRY_KEY)
var control = make(map[string]chan string)

var Logger = log.New(os.Stdout, "workers: ", log.Ldate|log.Lmicroseconds)

var Middleware = newMiddleware(
	&MiddlewareLogging{},
	&MiddlewareRetry{},
)

func Process(queue string, job jobFunc, concurrency int) {
	managers[queue] = newManager(queue, job, concurrency)
}

func Run() {
	schedule.start()

	for _, manager := range managers {
		manager.start()
	}

	go listenForSignals()

	waitForExit()
}

func Quit() {
	for _, m := range managers {
		go (func(m *manager) { m.quit() })(m)
	}

	schedule.quit()
}

func listenForSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGUSR1, syscall.SIGINT, syscall.SIGTERM)

	for sig := range signals {
		switch sig {
		case syscall.SIGINT, syscall.SIGUSR1, syscall.SIGTERM:
			Quit()
		}
	}
}

func waitForExit() {
	for _, manager := range managers {
		manager.Wait()
	}
}
