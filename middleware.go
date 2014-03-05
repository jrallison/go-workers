package workers

type Action interface {
	Call(queue string, message *Msg, next func())
}

type Middlewares struct {
	actions []Action
}

func (m *Middlewares) Append(action Action) {
	m.actions = append(m.actions, action)
}

func (m *Middlewares) Prepend(action Action) {
	actions := make([]Action, len(m.actions)+1)
	actions[0] = action
	copy(actions[1:], m.actions)
	m.actions = actions
}

func (m *Middlewares) call(queue string, message *Msg, final func()) {
	continuation(m.actions, queue, message, final)()
}

func continuation(actions []Action, queue string, message *Msg, final func()) func() {
	return func() {
		if len(actions) > 0 {
			actions[0].Call(
				queue,
				message,
				continuation(actions[1:], queue, message, final),
			)
		} else {
			final()
		}
	}
}

func NewMiddleware(actions ...Action) *Middlewares {
	return &Middlewares{actions}
}
