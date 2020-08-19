package eventstore

import (
	"context"

	event "github.com/plgd-dev/cqrs/event"
)

// VersionQuery used to load events from version.
type VersionQuery struct {
	AggregateId string //required
	Version     uint64 //required
}

// SnapshotQuery used to load events from snapshot.
type SnapshotQuery struct {
	GroupId           string //filter by group Id and it is used only when aggreagateId is empty
	AggregateId       string //filter to certain aggregateId
	SnapshotEventType string //required
}

// EventStore provides interface over eventstore. More aggregates can be grouped by groupId,
// but aggregateId of aggregates must be unique against whole DB.
type EventStore interface {
	Save(ctx context.Context, groupId string, aggregateId string, events []event.Event) (concurrencyException bool, err error)
	SaveSnapshot(ctx context.Context, groupId string, aggregateId string, event event.Event) (concurrencyException bool, err error)
	LoadUpToVersion(ctx context.Context, queries []VersionQuery, eventHandler event.Handler) error
	LoadFromVersion(ctx context.Context, queries []VersionQuery, eventHandler event.Handler) error
	LoadFromSnapshot(ctx context.Context, queries []SnapshotQuery, eventHandler event.Handler) error
	RemoveUpToVersion(ctx context.Context, queries []VersionQuery) error
}

// GoroutinePoolGoFunc processes actions via provided function
type GoroutinePoolGoFunc func(func()) error
