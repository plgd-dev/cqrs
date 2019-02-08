package eventstore

import (
	"context"

	event "github.com/go-ocf/cqrs/event"
)

// Query used to load events - all members are optional.
type Query struct {
	GroupId     string
	AggregateId string
	Version     uint64
}

type QueryIter interface {
	Next(query *Query) bool
	Err() error
}

// QueryHandler provides handler for process query eventstore.
type QueryHandler interface {
	QueryHandle(ctx context.Context, iter QueryIter) (err error)
}

// EventStore provides interface over eventstore.
type EventStore interface {
	Save(ctx context.Context, groupId string, aggregateId string, events []event.Event) (concurrencyException bool, err error)
	Load(ctx context.Context, queries []Query, eventHandler event.Handler) error

	SaveSnapshotQuery(ctx context.Context, groupId string, aggregateId string, version uint64) error
	LoadSnapshotQueries(ctx context.Context, queries []Query, qh QueryHandler) error
}
