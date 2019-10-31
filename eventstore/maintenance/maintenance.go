package maintenance

import (
	"context"

	"github.com/go-ocf/cqrs/eventstore"
)

type MaintenanceEventStore interface {
	Insert(ctx context.Context, task eventstore.VersionQuery) error
	Query(ctx context.Context, limit int, taskHandler TaskHandler) error
	Remove(ctx context.Context, task eventstore.VersionQuery) error
}

type TaskHandler interface {
	Handle(ctx context.Context, iter Iter) (err error)
}

//Iter provides iterator over maintenance db records
type Iter interface {
	Next(ctx context.Context, task *eventstore.VersionQuery) bool
	Err() error
}
