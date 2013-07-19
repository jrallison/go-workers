package workers

type worker struct {
	manager *manager
	stop    chan bool
	exit    chan bool
}

func (w *worker) start() {
	go w.work(w.manager.fetch.Messages())
}

func (w *worker) quit() {
	w.stop <- true
	<-w.exit
}

func (w *worker) work(messages chan *Msg) {
	for {
		select {
		case message := <-messages:
			Middleware.call(message, func() {
				w.manager.job(message)
			})

			w.manager.confirm <- message
		case <-w.stop:
			w.exit <- true
			break
		}
	}
}

func newWorker(m *manager) *worker {
	return &worker{m, make(chan bool), make(chan bool)}
}
