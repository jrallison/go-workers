package workers

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

var called chan bool

func myJob(message *Msg) {
	called <- true
}

func TestWorkers(t *testing.T) {
	setupTestConfig()

	//allows running in tests
	called = make(chan bool)

	Process("myqueue", myJob, 10)

	Start()

	Enqueue("myqueue", "Add", []int{1, 2})
	<-called

	Quit()

	// TODO make this test more deterministic, randomly locks up in travis.
	//allows starting and stopping multiple times
	//	called = make(chan bool)

	//	Process("myqueue", myJob, 10)

	//	Start()
	//	Quit()

	//	Start()

	//	Enqueue("myqueue", "Add", []int{1, 2})
	//	<-called

	//	Quit()
	//

	//runs beforeStart hooks
	hooks := []string{}

	BeforeStart(func() {
		hooks = append(hooks, "1")
	})
	BeforeStart(func() {
		hooks = append(hooks, "2")
	})
	BeforeStart(func() {
		hooks = append(hooks, "3")
	})

	Start()

	assert.True(t, reflect.DeepEqual(hooks, []string{"1", "2", "3"}))

	Quit()

	// Clear out global hooks variable
	beforeStart = nil

	//runs beforeStart hooks"
	hooks = []string{}

	DuringDrain(func() {
		hooks = append(hooks, "1")
	})
	DuringDrain(func() {
		hooks = append(hooks, "2")
	})
	DuringDrain(func() {
		hooks = append(hooks, "3")
	})

	Start()

	assert.True(t, reflect.DeepEqual(hooks, []string{}))

	Quit()

	assert.True(t, reflect.DeepEqual(hooks, []string{"1", "2", "3"}))

	// Clear out global hooks variable
	duringDrain = nil
}
