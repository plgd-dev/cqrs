package event

import (
	"context"
)

//Event interface over event created by user.
type Event interface {
	Version() uint64
	EventType() string
}

//EventUnmarshaler provides event.
type EventUnmarshaler struct {
	Version     uint64
	EventType   string
	AggregateID string
	GroupID     string
	Unmarshal   func(v interface{}) error
}

//Iter provides iterator over events from eventstore or eventbus.
type Iter interface {
	Next(ctx context.Context, eventUnmarshaler *EventUnmarshaler) bool
	Err() error
}

// Handler provides handler for eventstore or eventbus.
type Handler interface {
	Handle(ctx context.Context, iter Iter) (err error)
}

//MarshalerFunc marshal struct to bytes.
type MarshalerFunc func(v interface{}) ([]byte, error)

//UnmarshalerFunc unmarshal bytes to pointer of struct.
type UnmarshalerFunc func(b []byte, v interface{}) error
