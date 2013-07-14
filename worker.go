package workers

type worker struct {
	manager *manager
}

func (w *worker) work(messages chan interface{}) {
	logger.Println("starting work for: ", w.manager.queue)

	for message := range messages {
		logger.Println("performing job with: ", message)
		w.manager.job(message)
	}
}

func newWorker(m *manager) *worker {
	return &worker{m}
}
