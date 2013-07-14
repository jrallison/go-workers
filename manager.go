package workers

type manager struct {
	queue       string
	fetch       fetcher
	job         jobFunc
	concurrency int
	messages    chan interface{}
}

func (m *manager) manage(c chan string) {
	logger.Println("managing queue: ", m.queue)

	for i := 0; i < m.concurrency; i++ {
		go newWorker(m).work(m.messages)
	}

	go m.fetch.Fetch()

	for {
		logger.Println("selecting")
		select {
		case message := <-m.fetch.Messages():
			logger.Println("fetched message: ", message)
			m.messages <- message
		case control := <-c:
			logger.Println("received control: ", control)
			// stop fetching from redis if told to prepare
			// close messages channel if told to quit
		}
	}

	logger.Println("quitting manager")
}

func newManager(queue string, job jobFunc, concurrency int) *manager {
	m := &manager{
		queue,
		nil,
		job,
		concurrency,
		make(chan interface{}),
	}

	m.fetch = newFetch(m)

	return m
}
