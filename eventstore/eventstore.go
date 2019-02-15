package eventstore

import (
	"context"

	event "github.com/go-ocf/cqrs/event"
)

// QueryFromVersion used to load events from version.
type QueryFromVersion struct {
	AggregateId string //required
	Version     uint64 //required
}

// QueryFromSnapshot used to load events from snapshot.
type QueryFromSnapshot struct {
	GroupId           string //filter by group Id and it is used only when aggreagateId is empty
	AggregateId       string //filter to certain aggregateId
	SnapshotEventType string //required
}

// EventStore provides interface over eventstore. More aggregates can be grouped by groupId,
// but aggregateId of aggregates must be unique against whole DB.
type EventStore interface {
	Save(ctx context.Context, groupId string, aggregateId string, events []event.Event) (concurrencyException bool, err error)
	SaveSnapshot(ctx context.Context, groupId string, aggregateId string, event event.Event) (concurrencyException bool, err error)
	LoadFromVersion(ctx context.Context, queries []QueryFromVersion, eventHandler event.Handler) error
	LoadFromSnapshot(ctx context.Context, queries []QueryFromSnapshot, eventHandler event.Handler) error
}
