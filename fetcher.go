package workers

import (
	"github.com/garyburd/redigo/redis"
)

type fetcher interface {
	Fetch()
	Acknowledge(*interface{})
	Messages() chan *interface{}
}

type Fetch struct {
	manager  *manager
	messages chan *interface{}
	conn     redis.Conn
}

func (f *Fetch) Fetch() {
	logger.Println("starting to pull from redis queue")

	for {
		message, err := f.conn.Do("brpop", f.manager.queue, 1)
		if err != nil {
			logger.Println("ERR: ", err)
		}

		if message == nil {
			logger.Println("no message found. refetching...")
		} else {
			logger.Println("pulled message: ", message)
			f.messages <- &message
		}

	}
}

func (f *Fetch) Messages() chan *interface{} {
	return f.messages
}

func (f *Fetch) Acknowledge(*interface{}) {
	// noop
}

func newFetch(m *manager) fetcher {
	conn, err := redis.Dial("tcp", "localhost:6379")

	if err != nil {
		logger.Println("ERR: couldn't connect to redis. ", err)
	}

	return &Fetch{
		m,
		make(chan *interface{}),
		conn,
	}
}
