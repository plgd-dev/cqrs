package eventstore

import (
	"context"

	event "github.com/plgd-dev/cqrs/event"
)

// VersionQuery used to load events from version.
type VersionQuery struct {
	GroupID     string //required
	AggregateID string //required
	Version     uint64 //required
}

// SnapshotQuery used to load events from snapshot.
type SnapshotQuery struct {
	GroupID           string //filter by group ID
	AggregateID       string //filter to certain aggregateID, groupID is required
	SnapshotEventType string //required
}

// EventStore provides interface over eventstore. More aggregates can be grouped by groupID,
// but aggregateID of aggregates must be unique against whole DB.
type EventStore interface {
	Save(ctx context.Context, groupID string, aggregateID string, events []event.Event) (concurrencyException bool, err error)
	SaveSnapshot(ctx context.Context, groupID string, aggregateID string, event event.Event) (concurrencyException bool, err error)
	LoadUpToVersion(ctx context.Context, queries []VersionQuery, eventHandler event.Handler) error
	LoadFromVersion(ctx context.Context, queries []VersionQuery, eventHandler event.Handler) error
	LoadFromSnapshot(ctx context.Context, queries []SnapshotQuery, eventHandler event.Handler) error
	RemoveUpToVersion(ctx context.Context, queries []VersionQuery) error
}

// GoroutinePoolGoFunc processes actions via provided function
type GoroutinePoolGoFunc func(func()) error
