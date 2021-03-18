package workers

import (
	"github.com/alicebob/miniredis"
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

func ConfigSpec(c gospec.Context) {
	var recoverOnPanic = func(f func()) (err interface{}) {
		defer func() {
			if cause := recover(); cause != nil {
				err = cause
			}
		}()

		f()

		return
	}

	c.Specify("can specify custom process", func() {
		mr, _ := miniredis.Run()
		defer mr.Close()

		c.Expect(Config.processId, Equals, "1")

		Configure(map[string]string{
			"server":  mr.Addr(),
			"process": "2",
		})

		c.Expect(Config.processId, Equals, "2")
	})

	c.Specify("requires a server parameter", func() {
		err := recoverOnPanic(func() {
			Configure(map[string]string{"process": "2"})
		})

		c.Expect(err, Equals, "Configure requires a 'server' option, which identifies a Redis instance")
	})

	c.Specify("requires a process parameter", func() {
		err := recoverOnPanic(func() {
			Configure(map[string]string{"server": "localhost:8000"})
		})

		c.Expect(err, Equals, "Configure requires a 'process' option, which uniquely identifies this instance")
	})

	c.Specify("adds ':' to the end of the namespace", func() {
		mr, _ := miniredis.Run()
		defer mr.Close()

		c.Expect(Config.Namespace, Equals, "{worker}:")

		Configure(map[string]string{
			"server":    mr.Addr(),
			"process":   "1",
			"namespace": "prod", // no matter set namespace. it must be {worker}:
		})

		c.Expect(Config.Namespace, Equals, "{worker}:")
	})

	c.Specify("defaults poll interval to 15 seconds", func() {
		mr, _ := miniredis.Run()
		defer mr.Close()

		Configure(map[string]string{
			"server":  mr.Addr(),
			"process": "1",
		})

		c.Expect(Config.PollInterval, Equals, 15)
	})

	c.Specify("allows customization of poll interval", func() {
		mr, _ := miniredis.Run()
		defer mr.Close()

		Configure(map[string]string{
			"server":        mr.Addr(),
			"process":       "1",
			"poll_interval": "1",
		})

		c.Expect(Config.PollInterval, Equals, 1)
	})
}
