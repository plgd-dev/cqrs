package eventstore

import (
	"context"

	event "github.com/go-ocf/cqrs/event"
)

// QueryFromVersion used to load events from snapshot.
type QueryFromVersion struct {
	AggregateId string //tlo certain aggregateId
	Version     uint64 //required
}

// QueryFromSnapshot used to load events from snapshot.
type QueryFromSnapshot struct {
	GroupId           string //filter by groupId is used only when aggagateId is empty
	AggregateId       string //filter to certain aggregateId
	SnapshotEventType string //required
}

// EventStore provides interface over eventstore.
type EventStore interface {
	Save(ctx context.Context, groupId string, aggregateId string, events []event.Event) (concurrencyException bool, err error)
	SaveSnapshot(ctx context.Context, groupId string, aggregateId string, event event.Event) (concurrencyException bool, err error)
	LoadFromVersion(ctx context.Context, queries []QueryFromVersion, eventHandler event.Handler) error
	LoadFromSnapshot(ctx context.Context, queries []QueryFromSnapshot, eventHandler event.Handler) error
}
