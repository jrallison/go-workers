package workers

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
)

type Fetcher interface {
	Fetch()
	Acknowledge(*Msg)
	Messages() chan *Msg
	Close()
	Closed() bool
}

type fetch struct {
	manager  *manager
	messages chan *Msg
	stop     chan bool
	exit     chan bool
	closed   bool
}

func (f *fetch) processOldMessages() {
	messages := f.inprogressMessages()

	for _, message := range messages {
		f.sendMessage(message)
	}
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
				logger.Println("ERR: ", err)
			} else {
				c <- message
			}
		}
	})(messages)

	for {
		select {
		case message := <-messages:
			f.sendMessage(message)
		case <-f.stop:
			f.closed = true
			f.exit <- true
			break
		}
	}
}

func (f *fetch) sendMessage(message string) {
	msg, err := NewMsg(message)

	if err != nil {
		logger.Println("ERR: Couldn't create message from", message, ":", err)
		return
	}

	f.Messages() <- msg
}

func (f *fetch) Acknowledge(message *Msg) {
	conn := Config.pool.Get()
	defer conn.Close()
	conn.Do("lrem", f.inprogressQueue(), -1, message.ToJson())
}

func (f *fetch) Messages() chan *Msg {
	return f.messages
}

func (f *fetch) Close() {
	f.stop <- true
	<-f.exit
}

func (f *fetch) Closed() bool {
	return f.closed
}

func (f *fetch) inprogressMessages() []string {
	conn := Config.pool.Get()
	defer conn.Close()

	messages, err := redis.Strings(conn.Do("lrange", f.inprogressQueue(), 0, -1))
	if err != nil {
		logger.Println("ERR: ", err)
	}

	return messages
}

func (f *fetch) inprogressQueue() string {
	return fmt.Sprint(f.manager.queue, ":", Config.processId, ":inprogress")
}

func newFetch(m *manager) Fetcher {
	return &fetch{
		m,
		make(chan *Msg),
		make(chan bool),
		make(chan bool),
		false,
	}
}
