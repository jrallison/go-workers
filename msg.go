package workers

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"

	simplejson "github.com/bitly/go-simplejson"
)

type Msgs []*Msg

func NewMsgs(messages []string) ([]*Msg, error) {
	msgs := make(Msgs, 0, len(messages))

	for _, m := range messages {
		if m, err := NewMsgFromString(m); err != nil {
			return nil, err
		} else {
			msgs = append(msgs, m)
		}
	}

	return msgs, nil
}

type data struct {
	*simplejson.Json
	data []byte
}

func (d data) ToJson() string {
	return string(d.data)
}

// For gospec only.
func (d *data) Equals(other interface{}) bool {
	otherJson := reflect.ValueOf(other).MethodByName("ToJson").Call([]reflect.Value{})
	return d.ToJson() == otherJson[0].String()
}

type Msg struct {
	jid        string
	Retry      bool
	RetryMax   int
	queue      string
	enqueuedAt float64
	error      string
	retryCount int
	failedAt   string
	retriedAt  string

	// The original json if the message was created with NewMsgFromString
	original string

	// The job arguments.
	args *data
}

var defaultArgs = []byte("[]")
var defaultArgsJson *simplejson.Json

func init() {
	defaultArgsJson, _ = simplejson.NewJson(defaultArgs)
}

type Args struct {
	*data
}

func (m *Msg) Jid() string {
	return m.jid
}

func (m *Msg) Args() *Args {
	if m.args == nil {
		m.args = &data{defaultArgsJson, defaultArgs}
	}
	return &Args{m.args}
}

func (m *Msg) OriginalJson() string {
	return m.original
}

func (m *Msg) ToJson() string {
	args := m.Args()
	d := map[string]interface{}{
		"jid":           m.jid,
		"queue":         m.queue,
		"retry_count":   m.retryCount,
		"enqueued_at":   m.enqueuedAt,
		"error_message": m.error,
		"failed_at":     m.failedAt,
		"retried_at":    m.retriedAt,
		"args":          args.Json,
	}
	if m.RetryMax > 0 {
		d["retry"] = json.Number(strconv.Itoa(m.RetryMax))
	} else {
		d["retry"] = m.Retry
	}
	json, _ := json.Marshal(d)

	return string(json)
}

// NsgMsgFromString creates a new message from the redis retry queue.
func NewMsgFromString(str string) (*Msg, error) {
	json, err := simplejson.NewJson([]byte(str))
	if err != nil {
		return nil, fmt.Errorf("Couldn't create message from %v: %v", str, err)
	}

	m := &Msg{
		jid:        json.Get("jid").MustString(),
		queue:      json.Get("queue").MustString(),
		enqueuedAt: json.Get("enqueued_at").MustFloat64(),
		error:      json.Get("error_message").MustString(),
		failedAt:   json.Get("failed_at").MustString(),
		retriedAt:  json.Get("retried_at").MustString(),
		original:   str,
	}

	if args := json.Get("args"); args.Interface() != nil {
		b, _ := args.MarshalJSON()
		m.args = &data{args, b}
	}

	if param, err := json.Get("retry_count").Int(); err == nil {
		m.retryCount = param
	} else {
		m.retryCount = -1
	}

	// Retry is either a bool or a number for hysterical reasons.
	if param, err := json.Get("retry").Bool(); err == nil {
		m.Retry = param
	} else if param, err := json.Get("retry").Int(); err == nil {
		m.RetryMax = param
		m.Retry = true
	}

	return m, nil
}

// NewMsg creates a new virgin message.
func NewMsg(jid string, args *simplejson.Json, b []byte) *Msg {
	m := &Msg{
		jid:        jid,
		Retry:      true,
		retryCount: -1,
	}
	if args != nil {
		m.args = &data{args, b}
	}
	return m
}
