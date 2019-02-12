package eventstore

import (
	"context"

	event "github.com/go-ocf/cqrs/event"
)

// Query used to load events - all members are optional.
type Query struct {
	GroupId               string //filter by groupId is used only when aggagateId is empty
	AggregateId           string //filter to certain aggregateId
	FromSnapshotEventType string //used if it is no empty, otherwise it use FromVersion
	FromVersion           uint64
}

// EventStore provides interface over eventstore.
type EventStore interface {
	Save(ctx context.Context, groupId string, aggregateId string, events []event.Event) (concurrencyException bool, err error)
	SaveSnapshot(ctx context.Context, groupId string, aggregateId string, event event.Event) (concurrencyException bool, err error)
	Load(ctx context.Context, queries []Query, eventHandler event.Handler) error
}
