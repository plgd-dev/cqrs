package event

import (
	"context"

	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
)

type Event interface {
	Version() uint64
	EventType() string
	AggregateId() string
}

type EventUnmarshaler interface {
	Event
	Unmarshal(v interface{}) error
}

type EventHandler interface {
	HandleEvent(ctx context.Context, path protoEvent.Path, eventDecoder EventUnmarshaler) (err error)
}

type MarshalerFunc func(v interface{}) ([]byte, error)
type UnmarshalerFunc func(b []byte, v interface{}) error
