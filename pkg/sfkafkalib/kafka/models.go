package kafka

import (
	"fmt"
)

type Error struct {
	Err error
	Val string
}

func (e Error) Error() string {
	return fmt.Sprintf("%v\n%s", e.Err, e.Val)
}

func (e Error) Unwrap() error {
	return e.Err
}
