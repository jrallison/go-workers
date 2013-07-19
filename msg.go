package workers

import (
	"github.com/bitly/go-simplejson"
	"reflect"
)

type data struct {
	*simplejson.Json
}

type Msg struct {
	*data
}

type Args struct {
	*data
}

func (m *Msg) Jid() string {
	return m.Get("jid").MustString()
}

func (m *Msg) Retry() bool {
	if retry, err := m.Get("retry").Bool(); err == nil {
		return retry
	}

	if retry, err := m.Get("retry").Int(); err == nil {
		return retry > 0
	}

	return false
}

func (m *Msg) Args() *Args {
	if args, ok := m.CheckGet("args"); ok {
		return &Args{&data{args}}
	} else {
		d, _ := newData("[]")
		return &Args{d}
	}
}

func (d *data) ToJson() string {
	json, _ := d.Encode()
	// TODO handle error
	return string(json)
}

func (d *data) Equals(other interface{}) bool {
	otherJson := reflect.ValueOf(other).MethodByName("ToJson").Call([]reflect.Value{})
	return d.ToJson() == otherJson[0].String()
}

func NewMsg(content string) (*Msg, error) {
	if d, err := newData(content); err != nil {
		return nil, err
	} else {
		return &Msg{d}, nil
	}
}

func newData(content string) (*data, error) {
	if json, err := simplejson.NewJson([]byte(content)); err != nil {
		return nil, err
	} else {
		return &data{json}, nil
	}
}
