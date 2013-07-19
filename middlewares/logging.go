package middlewares

import (
	"fmt"
)

type Logging struct{}

func (l *Logging) Call(message interface{}, next func()) {
	fmt.Println("Before")
	next()
	fmt.Println("After")
}
