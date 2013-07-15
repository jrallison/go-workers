package workers

import (
	"log"
	"os"
)

var managers = make(map[string]*manager)
var control = make(map[string]chan string)
var logger = log.New(os.Stdout, "background: ", log.Ldate|log.Lmicroseconds)

func Process(queue string, job jobFunc, concurrency int) {
	managers[queue] = newManager(queue, job, concurrency)
	managers[queue].start()
}

func Run() {
	for _, manager := range managers {
		manager.Wait()
	}
}
