package workers

type Action interface {
	Call(message interface{}, next func())
}

type middleware struct {
	actions []Action
}

func (m *middleware) Append(action Action) {
	m.actions = append(m.actions, action)
}

func (m *middleware) Prepend(action Action) {
	actions := make([]Action, len(m.actions)+1)
	actions[0] = action
	copy(actions[1:], m.actions)
	m.actions = actions
}

func (m *middleware) call(message *Msg, final func()) {
	continuation(m.actions, message, final)()
}

func continuation(actions []Action, message *Msg, final func()) func() {
	return func() {
		if len(actions) > 0 {
			actions[0].Call(message, continuation(actions[1:], message, final))
		} else {
			final()
		}
	}
}

func newMiddleware(actions ...Action) *middleware {
	return &middleware{actions}
}
