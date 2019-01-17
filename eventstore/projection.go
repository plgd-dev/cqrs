package eventstore

import (
	"context"
	"fmt"

	"github.com/go-ocf/cqrs/event"
	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
)

// Model user defined model where events from eventstore will be projected.
type Model interface {
	event.EventHandler
	SnapshotEventType() string
}

// FactoryModelFunc creates user model.
type FactoryModelFunc func(ctx context.Context) (Model, error)

// Projection projects events from eventstore to user model.
type Projection struct {
	path                protoEvent.Path
	numEventsInSnapshot int

	store EventStore

	factoryModel FactoryModelFunc
}

// MakeProjection creates projection over eventstore.
func MakeProjection(path protoEvent.Path, numEventsInSnapshot int, store EventStore, factoryModel FactoryModelFunc) Projection {
	return Projection{
		path:                path,
		numEventsInSnapshot: numEventsInSnapshot,
		store:               store,
		factoryModel:        factoryModel,
	}
}

type snapshotModel struct {
	model         Model
	snapshotOccur bool
	isEmpty       bool
	lastVersion   uint64
}

type iterator struct {
	iter        event.Iter
	num         int
	lastVersion uint64
	model       Model
}

func (i *iterator) Next(e *event.EventUnmarshaler) bool {
	if i.num > 0 {
		if i.iter.Next(e) {
			i.num++
			i.lastVersion = e.Version
			return true
		}
		return false
	}
	for i.iter.Next(e) {
		switch {
		case e.EventType == i.model.SnapshotEventType() || e.Version == 0:
			i.num++
			i.lastVersion = e.Version
			return true
		}
	}
	return false
}

func (i *iterator) Err() error {
	return i.iter.Err()
}

func (m *snapshotModel) HandleEventFromStore(ctx context.Context, path protoEvent.Path, iter event.Iter) (int, error) {
	m.isEmpty = false
	i := iterator{
		model: m.model,
		iter:  iter,
	}
	err := m.model.HandleEvent(ctx, path, &i)
	m.lastVersion = i.lastVersion
	return i.num, err
}

func (p Projection) loadEvents(ctx context.Context, model Model) (int, uint64, error) {
	var numEvents int
	var err error
	sm := snapshotModel{model: model, isEmpty: true}
	if p.numEventsInSnapshot <= 0 {
		numEvents, err = p.store.Load(ctx, p.path, &sm)
		if err != nil {
			return -1, 0, fmt.Errorf("aggregate cannot load events to model: %v", err)
		}
	} else {
		for i := 1; numEvents == 0 && i < 8; i++ {
			numEvents, err = p.store.LoadLatest(ctx, p.path, p.numEventsInSnapshot*i, &sm)
			if err != nil {
				return -1, 0, fmt.Errorf("aggregate cannot load last events to model: %v", err)
			}
			if sm.isEmpty {
				return 0, 0, nil
			}
		}
		if numEvents == 0 {
			numEvents, err = p.store.Load(ctx, p.path, &sm)
		}
	}
	return numEvents, sm.lastVersion, err
}

// Project returns updated user models by events.
func (p Projection) Project(ctx context.Context) (model Model, numEvents int, lastVersion uint64, err error) {
	model, err = p.factoryModel(ctx)
	if err != nil {
		return nil, -1, 0, fmt.Errorf("cannot project evenstore to model %v", err)
	}
	numEvents, lastVersion, err = p.loadEvents(ctx, model)
	return
}
