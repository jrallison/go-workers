package workers

import (
	"github.com/bitly/go-simplejson"
)

type Msg struct {
	*simplejson.Json
}

func (m *Msg) ToJson() string {
	json, _ := m.Encode()
	// TODO handle error
	return string(json)
}

func (m *Msg) Equals(other interface{}) bool {
	return m.ToJson() == other.(*Msg).ToJson()
}

func NewMsg(content string) (*Msg, error) {
	if json, err := simplejson.NewJson([]byte(content)); err != nil {
		return nil, err
	} else {
		return &Msg{json}, nil
	}
}
