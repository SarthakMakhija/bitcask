package clock

import "time"

type Clock interface {
	Now() int
}

type SystemClock struct{}

func NewSystemClock() *SystemClock {
	return &SystemClock{}
}

func (clock *SystemClock) Now() int {
	return int(time.Now().UnixNano())
}
