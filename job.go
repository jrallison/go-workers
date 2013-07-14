package workers

type workerJob interface {
	Perform(*interface{}) bool
}

type Job struct {
}

func (*Job) Perform(message *interface{}) bool {
	return true
}
