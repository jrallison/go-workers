package workers

import (
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Fetcher interface {
	Queue() string
	Fetch()
	Acknowledge(Msgs)
	Ready() chan bool
	FinishedWork() chan bool
	Messages() chan Msgs
	Close()
	Closed() bool
}

type fetch struct {
	queue        string
	ready        chan bool
	finishedwork chan bool
	messages     chan Msgs
	stop         chan bool
	exit         chan bool
	closed       chan bool
}

func NewFetch(queue string, messages chan Msgs, ready chan bool) Fetcher {
	return &fetch{
		queue,
		ready,
		make(chan bool),
		messages,
		make(chan bool),
		make(chan bool),
		make(chan bool),
	}
}

func (f *fetch) Queue() string {
	return f.queue
}

func (f *fetch) processOldMessages() {
	messages := f.inprogressMessages()

	for _, message := range messages {
		<-f.Ready()
		f.sendMessages([]string{message})
	}
}

func (f *fetch) Fetch() {
	messages := make(chan []string)

	f.processOldMessages()

	go func(c chan []string) {
		for {
			// f.Close() has been called
			if f.Closed() {
				break
			}
			<-f.Ready()
			f.tryFetchMessage(c)
		}
	}(messages)

	f.handleMessages(messages)
}

func (f *fetch) handleMessages(messages chan []string) {
	for {
		select {
		case msgs := <-messages:
			f.sendMessages(msgs)
		case <-f.stop:
			// Stop the redis-polling goroutine
			close(f.closed)
			// Signal to Close() that the fetcher has stopped
			close(f.exit)
			return
		}
	}
}

func (f *fetch) tryFetchMessage(messages chan []string) {
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
		messages <- []string{message}
	}
}

func (f *fetch) sendMessages(messages []string) {
	msgs, err := NewMsgs(messages)
	if err != nil {
		Logger.Println("ERR:", err)
		return
	}

	f.Messages() <- msgs
}

func (f *fetch) Acknowledge(messages Msgs) {
	conn := Config.Pool.Get()
	defer conn.Close()

	// TODO optimize with a redis lua script
	for _, m := range messages {
		conn.Do("lrem", f.inprogressQueue(), -1, m.OriginalJson())
	}
}

func (f *fetch) Messages() chan Msgs {
	return f.messages
}

func (f *fetch) Ready() chan bool {
	return f.ready
}

func (f *fetch) FinishedWork() chan bool {
	return f.finishedwork
}

func (f *fetch) Close() {
	f.stop <- true
	<-f.exit
}

func (f *fetch) Closed() bool {
	select {
	case <-f.closed:
		return true
	default:
		return false
	}
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
