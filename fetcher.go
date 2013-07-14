package workers

import (
	"github.com/garyburd/redigo/redis"
)

type fetcher interface {
	Fetch()
	Acknowledge(*interface{})
	Messages() chan *string
}

type Fetch struct {
	manager  *manager
	messages chan *string
	conn     redis.Conn
}

func (f *Fetch) Fetch() {
	logger.Println("starting to pull from redis queue")

	for {
		message, err := redis.Strings(f.conn.Do("brpop", f.manager.queue, 1))

		if err != nil {
			logger.Println("ERR: ", err)
		} else {
			logger.Println("pulled message: ", message)
			f.messages <- &message[1]
		}

	}
}

func (f *Fetch) Messages() chan *string {
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
		make(chan *string),
		conn,
	}
}
