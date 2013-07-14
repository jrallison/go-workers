package workers

type jobFunc func(message interface{}) bool

func Job(message interface{}) bool {
	return true
}
