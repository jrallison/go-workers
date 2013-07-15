package workers

import (
	"sync"
)

type manager struct {
	queue       string
	fetch       Fetcher
	job         jobFunc
	concurrency int
	workers     []*worker
	confirm     chan string
	stop        chan bool
	exit        chan bool
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
	m.prepare()

	for _, worker := range m.workers {
		worker.quit()
	}

	m.stop <- true
	<-m.exit

	m.Done()
}

func (m *manager) manage() {
	logger.Println("starting to manage: ", m.queue)

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

func newManager(queue string, job jobFunc, concurrency int) *manager {
	m := &manager{
		queue,
		nil,
		job,
		concurrency,
		make([]*worker, concurrency),
		make(chan string),
		make(chan bool),
		make(chan bool),
		&sync.WaitGroup{},
	}

	m.fetch = newFetch(m)

	return m
}
