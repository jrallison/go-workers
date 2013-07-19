package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

func MsgSpec(c gospec.Context) {
	c.Specify("NewMsg", func() {
		c.Specify("unmarshals json", func() {
			msg, _ := NewMsg("{\"hello\":\"world\",\"foo\":3}")
			hello, _ := msg.Get("hello").String()
			foo, _ := msg.Get("foo").Int()

			c.Expect(hello, Equals, "world")
			c.Expect(foo, Equals, 3)
		})

		c.Specify("returns an error if invalid json", func() {
			msg, err := NewMsg("{\"hello:\"world\",\"foo\":3}")

			c.Expect(msg, IsNil)
			c.Expect(err, Not(IsNil))
		})
	})
}
