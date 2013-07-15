package workers

import (
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
	conn     redis.Conn
	stop     chan bool
	exit     chan bool
	closed   bool
}

func (f *fetch) Fetch() {
	logger.Println("starting to pull from redis queue")

	messages := make(chan string)

	go (func(c chan string) {
		for {
			if f.Closed() {
				break
			}

			message, err := redis.Strings(f.conn.Do("brpop", f.manager.queue, 1))

			if err != nil {
				logger.Println("ERR: ", err, f)
			} else {
				logger.Println("sending message: ", message)
				c <- message[1]
			}
		}
	})(messages)

	for {
		select {
		case message := <-messages:
			logger.Println("pulled message: ", message)
			f.Messages() <- message
		case <-f.stop:
			f.closed = true
			f.exit <- true
			break
		}
	}
}

func (f *fetch) Acknowledge(string) {
	// noop
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

func newFetch(m *manager) Fetcher {
	conn, err := redis.Dial("tcp", "localhost:6400")

	if err != nil {
		logger.Println("ERR: couldn't connect to redis. ", err)
	}

	return &fetch{
		m,
		make(chan string),
		conn,
		make(chan bool),
		make(chan bool),
		false,
	}
}
