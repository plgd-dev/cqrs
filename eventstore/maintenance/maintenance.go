package maintenance

import (
	"context"
)

type MaintenanceEventStore interface {
	Insert(ctx context.Context, task Task) error
	Query(ctx context.Context, limit int, taskHandler TaskHandler) error
	Remove(ctx context.Context, task Task) error
}

type Task struct {
	AggregateID string
	Version     uint64
}

type TaskHandler interface {
	Handle(ctx context.Context, iter Iter) (err error)
}

//Iter provides iterator over maintenance db records
type Iter interface {
	Next(ctx context.Context, task *Task) bool
	Err() error
}
