package workers

import (
	"errors"
	"fmt"
	"sync/atomic"
	"time"
)

type worker struct {
	manager    *manager
	stop       chan bool
	exit       chan bool
	currentMsg *Msg
	startedAt  int64
}

func (w *worker) start() {
	go w.work(w.manager.fetch.Messages())
}

func (w *worker) quit() {
	w.stop <- true
	<-w.exit
}

func (w *worker) work(messages chan *Msg) {
	for {
		select {
		case message := <-messages:
			atomic.StoreInt64(&w.startedAt, time.Now().UTC().Unix())
			w.currentMsg = message

			if w.process(message) == nil {
				w.manager.confirm <- message
			}

			atomic.StoreInt64(&w.startedAt, 0)
			w.currentMsg = nil

			// Attempt to tell fetcher we're finished.
			// Can be used when the fetcher has slept due
			// to detecting an empty queue to requery the
			// queue immediately if we finish work.
			select {
			case w.manager.fetch.FinishedWork() <- true:
			default:
			}
		case w.manager.fetch.Ready() <- true:
			// Signaled to fetcher that we're
			// ready to accept a message
		case <-w.stop:
			w.exit <- true
			return
		}
	}
}

func (w *worker) process(message *Msg) (err error) {
	defer func() {
		if e := recover(); e != nil {
			lerr, ok := e.(error)
			if !ok {
				err = errors.New(fmt.Sprintf("Unable to get error from recover(): %v", e))
			} else {
				err = lerr
			}
		}
	}()

	return w.manager.mids.call(w.manager.queueName(), message, func() error {
		return w.manager.job(message)
	})
}

func (w *worker) processing() bool {
	return atomic.LoadInt64(&w.startedAt) > 0
}

func newWorker(m *manager) *worker {
	return &worker{m, make(chan bool), make(chan bool), nil, 0}
}
