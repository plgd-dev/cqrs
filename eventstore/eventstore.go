package eventstore

import (
	"context"

	event "github.com/go-ocf/cqrs/event"
	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
)

// Handler handles events from eventstore. Set applied to true when snapshot was loaded and all events after.
type Handler interface {
	HandleEventFromStore(ctx context.Context, path protoEvent.Path, iter event.Iter) (int, error)
}

//PathIter iterator over paths
type PathIter interface {
	Next(path *protoEvent.Path) bool
	Err() error
}

// PathsHandler provide iterator to list of paths
type PathsHandler interface {
	HandlePaths(ctx context.Context, iter PathIter) error
}

// EventStore provides interface over eventstore.
type EventStore interface {
	Save(ctx context.Context, path protoEvent.Path, events []event.Event) (concurrencyException bool, err error)
	Load(ctx context.Context, path protoEvent.Path, eh Handler) (int, error)
	LoadLatest(ctx context.Context, path protoEvent.Path, count int, eh Handler) (int, error)
	LoadFromVersion(ctx context.Context, path protoEvent.Path, version uint64, eh Handler) (int, error)
	ListPaths(ctx context.Context, path protoEvent.Path, pathsHandler PathsHandler) error
}
