package cqrs

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventstore"
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
type Path2TopicsFunc func(groupId, aggregateId string, event event.Event) []string

// Aggregate holds data for Handle command
type Aggregate struct {
	groupId             string
	aggregateId         string
	numEventsInSnapshot int
	store               eventstore.EventStore
	factoryModel        func(ctx context.Context) (AggregateModel, error)
}

// NewAggregate creates aggregate. it load and store events created from commands
func NewAggregate(groupId, aggregateId string, numEventsInSnapshot int, store eventstore.EventStore, factoryModel func(ctx context.Context) (AggregateModel, error)) (*Aggregate, error) {
	if aggregateId == "" {
		return nil, errors.New("invalid aggregateId")
	}
	if store == nil {
		return nil, errors.New("invalid eventstore")
	}

	return &Aggregate{
		groupId:             groupId,
		aggregateId:         aggregateId,
		numEventsInSnapshot: numEventsInSnapshot,
		store:               store,
		factoryModel:        factoryModel,
	}, nil
}

func (a *Aggregate) saveEvents(ctx context.Context, numEvents int, model AggregateModel, events []event.Event) ([]event.Event, bool, error) {
	concurrencyException, err := a.store.Save(ctx, a.groupId, a.aggregateId, events)
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
		_, err = a.store.SaveSnapshot(ctx, a.groupId, a.aggregateId, snapshotEvent)
		if err == nil {
			events = append(events, snapshotEvent)
		}
	}

	return events, concurrencyException, err
}

type aggrIterator struct {
	iter              event.Iter
	lastVersion       uint64
	numEvents         int
	snapshotEventType string
}

func (i *aggrIterator) Next(ctx context.Context, event *event.EventUnmarshaler) bool {
	if !i.iter.Next(ctx, event) {
		return false
	}
	i.lastVersion = event.Version
	if event.EventType != i.snapshotEventType {
		i.numEvents++
	} else {
		i.numEvents = 0
	}
	return true
}

func (i *aggrIterator) Err() error {
	return i.iter.Err()
}

type aggrModel struct {
	model       eventstore.Model
	lastVersion uint64
	numEvents   int
}

func (ah *aggrModel) SnapshotEventType() string {
	return ah.model.SnapshotEventType()
}

func (ah *aggrModel) Handle(ctx context.Context, iter event.Iter) error {
	i := aggrIterator{
		iter:              iter,
		snapshotEventType: ah.SnapshotEventType(),
	}
	err := ah.model.Handle(ctx, &i)
	ah.lastVersion = i.lastVersion
	ah.numEvents = i.numEvents
	return err
}

// HandleCommand transforms command to a event, store and publish event.
func (a *Aggregate) HandleCommand(ctx context.Context, cmd Command) ([]event.Event, error) {
	for {
		model, err := a.factoryModel(ctx)
		if err != nil {
			return nil, fmt.Errorf("aggregate cannot create model: %v", err)
		}
		amodel := &aggrModel{model: model}
		ep := eventstore.NewProjection(a.store, func(ctx context.Context) (eventstore.Model, error) { return amodel, nil })
		err = ep.Project(ctx, []eventstore.QueryFromSnapshot{
			eventstore.QueryFromSnapshot{
				AggregateId:       a.aggregateId,
				GroupId:           a.groupId,
				SnapshotEventType: model.SnapshotEventType(),
			},
		})

		if err != nil {
			return nil, fmt.Errorf("aggregate cannot load model: %v", err)
		}
		if amodel.numEvents > 0 || amodel.lastVersion > 0 {
			amodel.lastVersion++
		}
		newEvents, err := model.HandleCommand(ctx, cmd, amodel.lastVersion)
		if err != nil {
			return nil, fmt.Errorf("aggregate model cannot handle command: %v", err)
		}

		saveEvents, concurrencyException, err := a.saveEvents(ctx, amodel.numEvents, model, newEvents)
		if concurrencyException {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("aggregate model cannot handle command: %v", err)
		}
		return saveEvents, err
	}
}
