package workers

type jobFunc func(message interface{}) bool
