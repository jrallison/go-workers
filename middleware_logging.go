package workers

import (
	"fmt"
)

type MiddlewareLogging struct{}

func (l *MiddlewareLogging) Call(queue string, message *Msg, next func()) {
	fmt.Println("Before")
	next()
	fmt.Println("After")
}
