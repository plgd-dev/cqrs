package eventstore

import (
	"context"
	"fmt"

	"github.com/go-ocf/cqrs/event"
)

// Model user defined model where events from eventstore will be projected.
type Model interface {
	event.Handler
}

// FactoryModelFunc creates user model.
type FactoryModelFunc func(ctx context.Context) (Model, error)

// Projection projects events from eventstore to user model.
type Projection struct {
	queries []Query

	store          EventStore
	loaderTreshold int

	factoryModel FactoryModelFunc
}

// MakeProjection creates projection over eventstore.
func MakeProjection(queries []Query, loaderTreshold int, store EventStore, factoryModel FactoryModelFunc) Projection {
	return Projection{
		queries:        queries,
		store:          store,
		factoryModel:   factoryModel,
		loaderTreshold: loaderTreshold,
	}
}

type loader struct {
	store     EventStore
	model     Model
	queries   []Query
	threshold int
}

func (l *loader) loadEvents(ctx context.Context) error {
	err := l.store.Load(ctx, l.queries, l.model)
	if err != nil {
		return fmt.Errorf("cannot load events to eventstore model: %v", err)
	}
	l.queries = l.queries[:0]
	return nil
}

func (l *loader) QueryHandle(ctx context.Context, iter QueryIter) error {
	var query Query
	for iter.Next(&query) {
		l.queries = append(l.queries, query)
		if len(l.queries) == l.threshold {
			err := l.loadEvents(ctx)
			if err != nil {
				return err
			}
		}
	}
	if iter.Err() != nil {
		return iter.Err()
	}
	if len(l.queries) > 0 {
		return l.loadEvents(ctx)
	}
	return nil
}

// Project returns updated user models by events.
func (p Projection) Project(ctx context.Context) (model Model, err error) {
	model, err = p.factoryModel(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot project evenstore to model %v", err)
	}
	return model, p.store.LoadSnapshotQueries(ctx, p.queries, &loader{
		store:     p.store,
		model:     model,
		threshold: p.loaderTreshold,
	})
}
