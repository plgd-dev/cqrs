package cqrs

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventstore"
	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
)

// Command user defined command that will handled in AggregateModel.HandleCommand
type Command interface{}

// AggregateModel user model for aggregate need to satisfy this interface.
type AggregateModel interface {
	eventstore.Model

	HandleCommand(ctx context.Context, cmd Command, newVersion uint64) ([]event.Event, error)
	TakeSnapshot(version uint64) (event.Event, error)
}

// Path2TopicsFunc convert path and event to topics where will be published event.
type Path2TopicsFunc func(path protoEvent.Path, event event.Event) []string

// Aggregate holds data for Handle command
type Aggregate struct {
	path                protoEvent.Path
	numEventsInSnapshot int
	store               eventstore.EventStore
	factoryModel        func(ctx context.Context) (AggregateModel, error)
}

// NewAggregate creates aggregate. it load and store events created from commands
func NewAggregate(path protoEvent.Path, store eventstore.EventStore, numEventsInSnapshot int, factoryModel func(ctx context.Context) (AggregateModel, error)) (*Aggregate, error) {
	if path.AggregateId == "" {
		return nil, errors.New("invalid aggregateId")
	}
	if store == nil {
		return nil, errors.New("invalid eventstore")
	}

	return &Aggregate{
		path:                path,
		numEventsInSnapshot: numEventsInSnapshot,
		store:               store,
		factoryModel:        factoryModel,
	}, nil
}

func (a *Aggregate) saveEvents(ctx context.Context, numEvents int, model AggregateModel, events []event.Event) ([]event.Event, bool, error) {
	concurrencyException, err := a.store.Save(ctx, a.path, events)
	if concurrencyException {
		return nil, concurrencyException, err
	}
	if err != nil {
		return nil, concurrencyException, fmt.Errorf("aggregate cannot save events: %v", err)
	}
	numEvents = numEvents + len(events)
	if a.numEventsInSnapshot > 0 && numEvents > a.numEventsInSnapshot {

		snapshotEvent, err := model.TakeSnapshot(events[len(events)-1].Version() + 1)
		if err != nil {
			// events are stored in eventstore that means we cannot resolve this issue
			return nil, false, nil
		}
		_, err = a.store.Save(ctx, a.path, []event.Event{snapshotEvent})
		if err == nil {
			events = append(events, snapshotEvent)
		}
	}

	return events, concurrencyException, err
}

// HandleCommand transforms command to a event, store and publish event.
func (a *Aggregate) HandleCommand(ctx context.Context, cmd Command) ([]event.Event, error) {
	for {
		model, err := a.factoryModel(ctx)
		if err != nil {
			return nil, fmt.Errorf("aggregate cannot create model: %v", err)
		}
		ep := eventstore.MakeProjection(a.path, a.numEventsInSnapshot, a.store, func(ctx context.Context) (eventstore.Model, error) { return model, nil })
		_, numEvents, lastVersion, err := ep.Project(ctx)

		if err != nil {
			return nil, fmt.Errorf("aggregate cannot load model: %v", err)
		}
		if numEvents > 0 {
			lastVersion++
		}
		newEvents, err := model.HandleCommand(ctx, cmd, lastVersion)
		if err != nil {
			return nil, fmt.Errorf("aggregate model cannot handle command: %v", err)
		}

		saveEvents, concurrencyException, err := a.saveEvents(ctx, numEvents, model, newEvents)
		if concurrencyException {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("aggregate model cannot handle command: %v", err)
		}
		return saveEvents, err
	}
}
