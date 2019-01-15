package eventstore

import (
	"context"

	event "github.com/go-ocf/cqrs/event"
	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
)

// Handler handles events from eventstore. Set applied to true when snapshot was loaded and all events after.
type Handler interface {
	HandleEventFromStore(ctx context.Context, path protoEvent.Path, eventDecoder event.EventUnmarshaler) (applied bool, err error)
}

// EventStore provides interface over eventstore.
type EventStore interface {
	Save(ctx context.Context, path protoEvent.Path, events []event.Event) (concurrencyException bool, err error)
	Load(ctx context.Context, path protoEvent.Path, eh Handler) (int, error)
	LoadLastEvents(ctx context.Context, path protoEvent.Path, numLastEvents int, eh Handler) (int, error)
	LoadFromVersion(ctx context.Context, path protoEvent.Path, version uint64, eh Handler) (int, error)
	ListPaths(ctx context.Context, path protoEvent.Path) ([]protoEvent.Path, error)
}
