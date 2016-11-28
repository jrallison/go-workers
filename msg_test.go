package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

func MsgSpec(c gospec.Context) {
	c.Specify("Args", func() {
		c.Specify("returns args key", func() {
			msg := buildVirginMessages("[\"foo\",\"bar\"]")[0]
			c.Expect(msg.Args().ToJson(), Equals, "[\"foo\",\"bar\"]")
		})

		c.Specify("returns empty array if args key doesn't exist", func() {
			msg, _ := NewMsgFromString("{}}")
			//"", nil, nil)
			c.Expect(msg.Args().ToJson(), Equals, "[]")
		})

		c.Specify("returns empty array if args key doesn't exist", func() {
			msg := NewMsg("", nil, nil)
			c.Expect(msg.Args().ToJson(), Equals, "[]")
		})
	})
}
