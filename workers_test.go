package workers

import (
	"github.com/customerio/gospec"
)

var called chan bool

func myJob(message *Msg) {
	called <- true
}

func WorkersSpec(c gospec.Context) {
	c.Specify("Workers", func() {
		c.Specify("allows running in tests", func() {
			called = make(chan bool)

			Process("myqueue", myJob, 10)

			Start()

			Enqueue("myqueue", "Add", []int{1, 2})
			<-called

			Quit()
		})
	})
}
