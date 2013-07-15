package workers

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
)

type Fetcher interface {
	Fetch()
	Acknowledge(string)
	Messages() chan string
	Close()
	Closed() bool
}

type fetch struct {
	manager  *manager
	messages chan string
	stop     chan bool
	exit     chan bool
	closed   bool
}

func (f *fetch) Fetch() {
	messages := make(chan string)

	f.processOldMessages()

	go (func(c chan string) {
		for {
			if f.Closed() {
				break
			}

			conn := Config.pool.Get()
			defer conn.Close()

			message, err := redis.String(conn.Do("brpoplpush", f.manager.queue, f.inprogressQueue(), 1))

			if err != nil {
				logger.Println("ERR: ", err, f)
			} else {
				c <- message
			}
		}
	})(messages)

	for {
		select {
		case message := <-messages:
			f.Messages() <- message
		case <-f.stop:
			f.closed = true
			f.exit <- true
			break
		}
	}
}

func (f *fetch) Acknowledge(message string) {
	conn := Config.pool.Get()
	defer conn.Close()
	conn.Do("lrem", f.inprogressQueue(), -1, message)
}

func (f *fetch) Messages() chan string {
	return f.messages
}

func (f *fetch) Close() {
	f.stop <- true
	<-f.exit
}

func (f *fetch) Closed() bool {
	return f.closed
}

func (f *fetch) processOldMessages() {
	oldMessages := f.inprogressMessages()

	if len(oldMessages) > 0 {
		for _, m := range oldMessages {
			f.Messages() <- m
		}
	}
}

func (f *fetch) inprogressMessages() []string {
	conn := Config.pool.Get()
	defer conn.Close()

	messages, err := redis.Strings(conn.Do("lrange", f.inprogressQueue(), 0, -1))
	if err != nil {
		logger.Println("ERR: ", err, f)
	}

	return messages
}

func (f *fetch) inprogressQueue() string {
	return fmt.Sprint(f.manager.queue, ":", Config.processId, ":inprogress")
}

func newFetch(m *manager) Fetcher {
	return &fetch{
		m,
		make(chan string),
		make(chan bool),
		make(chan bool),
		false,
	}
}
