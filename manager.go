package workers

import (
	"strings"
	"sync"
)

type manager struct {
	queue       string
	fetch       Fetcher
	job         jobFunc
	concurrency int
	workers     []*worker
	confirm     chan *Msg
	stop        chan bool
	exit        chan bool
	mids        *Middlewares
	*sync.WaitGroup
}

func (m *manager) start() {
	m.Add(1)
	go m.manage()
}

func (m *manager) prepare() {
	if !m.fetch.Closed() {
		m.fetch.Close()
	}
}

func (m *manager) quit() {
	Logger.Println("quitting queue", m.queueName(), "(waiting for", m.processing(), "/", len(m.workers), "workers).")
	m.prepare()

	for _, worker := range m.workers {
		worker.quit()
	}

	m.stop <- true
	<-m.exit

	m.Done()
}

func (m *manager) manage() {
	Logger.Println("processing queue", m.queueName(), "with", m.concurrency, "workers.")

	m.loadWorkers()

	go m.fetch.Fetch()

	for {
		select {
		case message := <-m.confirm:
			m.fetch.Acknowledge(message)
		case <-m.stop:
			m.exit <- true
			break
		}
	}
}

func (m *manager) loadWorkers() {
	for i := 0; i < m.concurrency; i++ {
		m.workers[i] = newWorker(m)
		m.workers[i].start()
	}
}

func (m *manager) processing() (count int) {
	for _, worker := range m.workers {
		if worker.processing() {
			count++
		}
	}

	return
}

func (m *manager) queueName() string {
	return strings.Replace(m.queue, "queue:", "", 1)
}

func newManager(queue string, job jobFunc, concurrency int, mids ...Action) *manager {
	var customMids *Middlewares
	if len(mids) == 0 {
		customMids = Middleware
	} else {
		customMids = NewMiddleware(Middleware.actions...)
		for _, m := range mids {
			customMids.Append(m)
		}
	}
	m := &manager{
		Config.Namespace + "queue:" + queue,
		nil,
		job,
		concurrency,
		make([]*worker, concurrency),
		make(chan *Msg),
		make(chan bool),
		make(chan bool),
		customMids,
		&sync.WaitGroup{},
	}

	m.fetch = Config.Fetch(m.queue)

	return m
}
