package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"

	simplejson "github.com/bitly/go-simplejson"
)

func buildFetch(queue string) Fetcher {
	manager := newManager(queue, nil, 1)
	fetch := manager.fetch
	go fetch.Fetch()
	return fetch
}

func buildVirginMessages(args ...string) Msgs {
	var msgs []*Msg
	for _, a := range args {
		b := []byte(a)
		j, _ := simplejson.NewJson(b)
		msgs = append(msgs, NewMsg("", j, b))
	}
	return msgs
}

func buildMessages(args ...string) Msgs {
	var s []string
	for _, a := range args {
		b := []byte(a)
		j, _ := simplejson.NewJson(b)
		m := NewMsg("", j, b)
		s = append(s, m.ToJson())
	}

	messages, _ := NewMsgs(s)
	return messages
}

func FetchSpec(c gospec.Context) {
	c.Specify("Config.Fetch", func() {
		c.Specify("it returns an instance of fetch with queue", func() {
			fetch := buildFetch("fetchQueue1")
			c.Expect(fetch.Queue(), Equals, "queue:fetchQueue1")
			fetch.Close()
		})
	})

	c.Specify("Fetch", func() {
		messages := buildMessages("{\"foo\":\"bar\"}")

		c.Specify("it puts messages from the queues on the messages channel", func() {
			fetch := buildFetch("fetchQueue2")

			conn := Config.Pool.Get()
			defer conn.Close()

			for _, m := range messages {
				conn.Do("lpush", "queue:fetchQueue2", m.ToJson())
			}

			fetch.Ready() <- true
			found := <-fetch.Messages()

			c.Expect(len(messages), Equals, 1)
			c.Expect(len(found), Equals, 1)
			c.Expect(messages[0].original, Equals, found[0].original)

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue2"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})

		c.Specify("places in progress messages on private queue", func() {
			fetch := buildFetch("fetchQueue3")

			conn := Config.Pool.Get()
			defer conn.Close()

			for _, m := range messages {
				conn.Do("lpush", "queue:fetchQueue3", m.ToJson())
			}

			fetch.Ready() <- true
			<-fetch.Messages()

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue3:1:inprogress"))
			c.Expect(len, Equals, 1)

			found, _ := redis.Strings(conn.Do("lrange", "queue:fetchQueue3:1:inprogress", 0, -1))
			c.Expect(found[0], Equals, messages[0].ToJson())

			fetch.Close()
		})

		c.Specify("removes in progress message when acknowledged", func() {
			fetch := buildFetch("fetchQueue4")

			conn := Config.Pool.Get()
			defer conn.Close()

			for _, m := range messages {
				conn.Do("lpush", "queue:fetchQueue4", m.ToJson())
			}

			fetch.Ready() <- true
			<-fetch.Messages()

			fetch.Acknowledge(messages)

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue4:1:inprogress"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})

		c.Specify("removes in progress message when serialized differently", func() {
			json := "{\"foo\":\"bar\",\"args\":[]}"
			messages, _ := NewMsgs([]string{json})

			c.Expect(json, Not(Equals), messages[0].ToJson())

			fetch := buildFetch("fetchQueue5")

			conn := Config.Pool.Get()
			defer conn.Close()

			conn.Do("lpush", "queue:fetchQueue5", json)

			fetch.Ready() <- true
			<-fetch.Messages()

			fetch.Acknowledge(messages)

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue5:1:inprogress"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})

		c.Specify("refires any messages left in progress from prior instance", func() {
			msgs := buildMessages("{\"foo\":\"bar2\"}", "{\"foo\":\"bar3\"}")

			conn := Config.Pool.Get()
			defer conn.Close()

			conn.Do("lpush", "queue:fetchQueue6:1:inprogress", messages[0].ToJson())
			conn.Do("lpush", "queue:fetchQueue6:1:inprogress", msgs[0].ToJson())
			conn.Do("lpush", "queue:fetchQueue6", msgs[1].ToJson())

			fetch := buildFetch("fetchQueue6")

			fetch.Ready() <- true
			c.Expect((<-fetch.Messages())[0].original, Equals, msgs[0].original)
			fetch.Ready() <- true
			c.Expect((<-fetch.Messages())[0].original, Equals, messages[0].original)
			fetch.Ready() <- true
			c.Expect((<-fetch.Messages())[0].original, Equals, msgs[1].original)

			fetch.Acknowledge(messages)
			fetch.Acknowledge(msgs)

			len, _ := redis.Int(conn.Do("llen", "queue:fetchQueue6:1:inprogress"))
			c.Expect(len, Equals, 0)

			fetch.Close()
		})
	})
}
