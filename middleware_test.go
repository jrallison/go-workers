package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
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

func MiddlewareSpec(c gospec.Context) {
	middleware := NewMiddleware()
	message, _ := NewMsg("{\"foo\":\"bar\"}")
	order = make([]string, 0)
	first := &m1{}
	second := &m2{}

	c.Specify("newMiddleware", func() {
		c.Specify("doesn't contain any middleware", func() {
			c.Expect(len(middleware.actions), Equals, 0)
		})

		c.Specify("can specify middleware when initializing", func() {
			middleware = NewMiddleware(first, second)
			c.Expect(middleware.actions[0], Equals, first)
			c.Expect(middleware.actions[1], Equals, second)
		})
	})

	c.Specify("Append", func() {
		c.Specify("adds function at the end of the list", func() {
			middleware.Append(first)
			middleware.Append(second)

			middleware.call("myqueue", message, func() {
				order = append(order, "job")
			})

			c.Expect(
				arrayCompare(order, []string{
					"m1 enter",
					"m2 enter",
					"job",
					"m2 leave",
					"m1 leave",
				}),
				IsTrue,
			)
		})
	})

	c.Specify("{repend", func() {
		c.Specify("adds function at the beginning of the list", func() {
			middleware.Prepend(first)
			middleware.Prepend(second)

			middleware.call("myqueue", message, func() {
				order = append(order, "job")
			})

			c.Expect(
				arrayCompare(order, []string{
					"m2 enter",
					"m1 enter",
					"job",
					"m1 leave",
					"m2 leave",
				}),
				IsTrue,
			)
		})
	})
}
