package workers

import (
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Fetcher interface {
	Queue() string
	Fetch()
	Acknowledge(*Msg)
	Ready() chan bool
	Messages() chan *Msg
	Close()
	Closed() bool
}

type fetch struct {
	queue    string
	ready    chan bool
	messages chan *Msg
	stop     chan bool
	exit     chan bool
	closed   bool
}

func NewFetch(queue string, messages chan *Msg, ready chan bool) Fetcher {
	return &fetch{
		queue,
		ready,
		messages,
		make(chan bool),
		make(chan bool),
		false,
	}
}

func (f *fetch) Queue() string {
	return f.queue
}

func (f *fetch) processOldMessages() {
	messages := f.inprogressMessages()

	for _, message := range messages {
		<-f.Ready()
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

			<-f.Ready()

			(func() {
				conn := Config.Pool.Get()
				defer conn.Close()

				message, err := redis.String(conn.Do("brpoplpush", f.queue, f.inprogressQueue(), 1))

				if err != nil {
					// If redis returns null, the queue is empty. Just ignore the error.
					if err.Error() != "redigo: nil returned" {
						Logger.Println("ERR: ", err)
						time.Sleep(1 * time.Second)
					}
				} else {
					c <- message
				}
			})()
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
		Logger.Println("ERR: Couldn't create message from", message, ":", err)
		return
	}

	f.Messages() <- msg
}

func (f *fetch) Acknowledge(message *Msg) {
	conn := Config.Pool.Get()
	defer conn.Close()
	conn.Do("lrem", f.inprogressQueue(), -1, message.OriginalJson())
}

func (f *fetch) Messages() chan *Msg {
	return f.messages
}

func (f *fetch) Ready() chan bool {
	return f.ready
}

func (f *fetch) Close() {
	f.stop <- true
	<-f.exit
}

func (f *fetch) Closed() bool {
	return f.closed
}

func (f *fetch) inprogressMessages() []string {
	conn := Config.Pool.Get()
	defer conn.Close()

	messages, err := redis.Strings(conn.Do("lrange", f.inprogressQueue(), 0, -1))
	if err != nil {
		Logger.Println("ERR: ", err)
	}

	return messages
}

func (f *fetch) inprogressQueue() string {
	return fmt.Sprint(f.queue, ":", Config.processId, ":inprogress")
}
