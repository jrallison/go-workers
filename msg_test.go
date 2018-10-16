package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewMsg(t *testing.T) {
	//unmarshals json
	msg, _ := NewMsg("{\"hello\":\"world\",\"foo\":3}")
	hello, _ := msg.Get("hello").String()
	foo, _ := msg.Get("foo").Int()

	assert.Equal(t, "world", hello)
	assert.Equal(t, 3, foo)

	//returns an error if invalid json
	msg, err := NewMsg("{\"hello:\"world\",\"foo\":3}")

	assert.Nil(t, msg)
	assert.NotNil(t, err)
}

func TestArgs(t *testing.T) {
	//returns args key
	msg, _ := NewMsg("{\"hello\":\"world\",\"args\":[\"foo\",\"bar\"]}")
	assert.Equal(t, "[\"foo\",\"bar\"]", msg.Args().ToJson())

	//returns empty array if args key doesn't exist
	msg, _ = NewMsg("{\"hello\":\"world\"}")
	assert.Equal(t, "[]", msg.Args().ToJson())
}
