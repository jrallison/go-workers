package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func arrayCompare(a1, a2 []string) bool {
	if len(a1) != len(a2) {
		return false
	}

	for i := 0; i < len(a1); i++ {
		if a1[i] != a2[i] {
			return false
		}
	}

	return true
}

var order = make([]string, 0)

type m1 struct{}
type m2 struct{}

func (m *m1) Call(queue string, message *Msg, next func() bool) (result bool) {
	order = append(order, "m1 enter")
	result = next()
	order = append(order, "m1 leave")
	return
}

func (m *m2) Call(queue string, message *Msg, next func() bool) (result bool) {
	order = append(order, "m2 enter")
	result = next()
	order = append(order, "m2 leave")
	return
}

func TestNewMiddleware(t *testing.T) {
	//no middleware
	middleware := NewMiddleware()
	assert.Equal(t, 0, len(middleware.actions))

	//middleware set when initializing
	first := &m1{}
	second := &m2{}
	middleware = NewMiddleware(first, second)
	assert.Equal(t, first, middleware.actions[0])
	assert.Equal(t, second, middleware.actions[1])
}

func TestAppendMiddleware(t *testing.T) {
	first := &m1{}
	second := &m2{}
	middleware := NewMiddleware()
	middleware.Append(first)
	middleware.Append(second)

	middleware.call("myqueue", message, func() {
		order = append(order, "job")
	})

	expectedOrder := []string{
		"m1 enter",
		"m2 enter",
		"job",
		"m2 leave",
		"m1 leave",
	}

	assert.Equal(t, expectedOrder, order)
}

func TestPrependMiddleware(t *testing.T) {
	middleware := NewMiddleware()
	message, _ := NewMsg("{\"foo\":\"bar\"}")
	order = make([]string, 0)
	first := &m1{}
	second := &m2{}

	middleware.Prepend(first)
	middleware.Prepend(second)

	middleware.call("myqueue", message, func() {
		order = append(order, "job")
	})

	expectedOrder := []string{
		"m2 enter",
		"m1 enter",
		"job",
		"m1 leave",
		"m2 leave",
	}

	assert.Equal(t, expectedOrder, order)
}
