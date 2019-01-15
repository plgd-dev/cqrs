package cqrs

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventbus"
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

	store       eventstore.EventStore
	publisher   eventbus.Publisher
	path2topics Path2TopicsFunc

	factoryModel func(ctx context.Context) (AggregateModel, error)
}

// MakeAggregate creates aggregate. publisher and path2topics are no required when events don't need to be published.
func MakeAggregate(path protoEvent.Path, numEventsInSnapshot int, store eventstore.EventStore, publisher eventbus.Publisher, path2topics Path2TopicsFunc, factoryModel func(ctx context.Context) (AggregateModel, error)) (Aggregate, error) {
	if path.AggregateId == "" {
		return Aggregate{}, errors.New("invalid aggregateId")
	}
	if store == nil {
		return Aggregate{}, errors.New("invalid eventstore")
	}
	if publisher == nil && path2topics != nil {
		return Aggregate{}, errors.New("invalid combination of publisher")
	}
	if publisher != nil && path2topics == nil {
		return Aggregate{}, errors.New("invalid combination of path2topics")
	}

	return Aggregate{
		path:                path,
		numEventsInSnapshot: numEventsInSnapshot,
		store:               store,
		factoryModel:        factoryModel,
		publisher:           publisher,
		path2topics:         path2topics,
	}, nil
}

func (a Aggregate) saveEvents(ctx context.Context, numEvents int, model AggregateModel, events []event.Event) (bool, error) {
	concurrencyException, err := a.store.Save(ctx, a.path, events)
	if !concurrencyException {
		if err != nil {
			return concurrencyException, fmt.Errorf("aggregate cannot save events: %v", err)
		}
		numEvents = numEvents + len(events)
		if a.numEventsInSnapshot > 0 && numEvents > a.numEventsInSnapshot {

			snapshotEvent, err := model.TakeSnapshot(events[len(events)-1].Version() + 1)
			if err != nil {
				// events are stored in eventstore that means we cannot resolve this issue
				return false, nil
			}
			_, err = a.store.Save(ctx, a.path, []event.Event{snapshotEvent})
			if err != nil {
				// events are stored in eventstore that means we cannot resolve this issue
				return false, nil
			}
		}
	}
	return concurrencyException, err
}

// HandleCommand transforms command to a event, store and publish event.
func (a Aggregate) HandleCommand(ctx context.Context, cmd Command) (storeError error, pubError error) {
	for {
		model, err := a.factoryModel(ctx)
		if err != nil {
			return fmt.Errorf("aggregate cannot create model: %v", err), nil
		}
		ep := eventstore.MakeProjection(a.path, a.numEventsInSnapshot, a.store, func(ctx context.Context) (eventstore.Model, error) { return model, nil })
		_, numEvents, lastVersion, err := ep.Project(ctx)

		if err != nil {
			return fmt.Errorf("aggregate cannot load model: %v", err), nil
		}
		if numEvents > 0 {
			lastVersion++
		}
		newEvents, err := model.HandleCommand(ctx, cmd, lastVersion)
		if err != nil {
			return fmt.Errorf("aggregate model cannot handle command: %v", err), nil
		}

		concurrencyException, err := a.saveEvents(ctx, numEvents, model, newEvents)
		if !concurrencyException {
			if err != nil {
				return fmt.Errorf("aggregate model cannot handle command: %v", err), nil
			}
			if a.publisher != nil {
				var errors []error
				for _, event := range newEvents {
					err = a.publisher.Publish(ctx, a.path2topics(a.path, event), a.path, event)
					if err != nil {
						errors = append(errors, err)
					}
				}
				if len(errors) > 0 {
					return nil, fmt.Errorf("cannot publish events: %v", errors)
				}
			}

			return nil, nil
		}
	}
}
