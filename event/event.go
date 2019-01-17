package event

import (
	"context"

	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
)

//Event interface over event created by user.
type Event interface {
	Version() uint64
	EventType() string
	AggregateId() string
}

//EventUnmarshaler provides event.
type EventUnmarshaler struct {
	Version     uint64
	EventType   string
	AggregateId string
	Unmarshal   func(v interface{}) error
}

//Iter provides iterator over events from eventstore or eventbus.
type Iter interface {
	Next(eventUnmarshaler *EventUnmarshaler) bool
	Err() error
}

// EventHandler provides handler for eventstore or eventbus.
type EventHandler interface {
	HandleEvent(ctx context.Context, path protoEvent.Path, iter Iter) (err error)
}

//MarshalerFunc marshal struct to bytes.
type MarshalerFunc func(v interface{}) ([]byte, error)

//UnmarshalerFunc unmarshal bytes to pointer of struct.
type UnmarshalerFunc func(b []byte, v interface{}) error
