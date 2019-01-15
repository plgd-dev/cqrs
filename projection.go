package cqrs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventbus"
	"github.com/go-ocf/cqrs/eventstore"
	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
	"github.com/gofrs/uuid"
)

type aggregateProjectionModel struct {
	lock         sync.Mutex
	version      uint64
	isFirstEvent bool
	model        eventstore.Model
	eventstore   eventstore.EventStore
}

// Projection project events to user defined model from evenstore and update it by events from subscriber.
type Projection struct {
	//immutable
	eventstore          eventstore.EventStore
	numEventsInSnapshot int
	ctx                 context.Context
	cancel              context.CancelFunc
	factoryModel        eventstore.FactoryModelFunc

	subscriber     eventbus.Subscriber
	subscriptionId string

	//mutable part
	lock                 sync.Mutex
	observer             eventbus.Observer
	aggregateProjections map[string]*aggregateProjectionModel
}

// NewProjection creates projection.
func NewProjection(ctx context.Context, eventstore eventstore.EventStore, numEventsInSnapshot int, subscriber eventbus.Subscriber, factoryModel eventstore.FactoryModelFunc) (*Projection, error) {
	if eventstore == nil {
		return nil, errors.New("invalid handle of event store")
	}

	projCtx, projCancel := context.WithCancel(ctx)

	rd := Projection{
		aggregateProjections: make(map[string]*aggregateProjectionModel),
		eventstore:           eventstore,
		ctx:                  projCtx,
		cancel:               projCancel,
		numEventsInSnapshot:  numEventsInSnapshot,
		factoryModel:         factoryModel,
		subscriber:           subscriber,
		subscriptionId:       uuid.Must(uuid.NewV4()).String(),
	}

	return &rd, nil
}

func path2string(path protoEvent.Path) string {
	var b bytes.Buffer
	for _, p := range path.Path {
		if b.Len() > 0 {
			b.WriteString("/")
		}
		b.WriteString(p)
	}
	b.WriteString("/")
	b.WriteString(path.AggregateId)
	return b.String()
}

func (ap *aggregateProjectionModel) SnapshotEventType() string { return ap.model.SnapshotEventType() }

func (ap *aggregateProjectionModel) HandleEventFromStore(ctx context.Context, path protoEvent.Path, eventUnmarshaler event.EventUnmarshaler) (bool, error) {
	//this function is called from HandleEven under lock -> we just apply call
	if eventUnmarshaler.Version() != eventUnmarshaler.Version()+1 {
		return false, fmt.Errorf("event version unexpected over jump events")
	}
	ap.version = eventUnmarshaler.Version()
	return true, ap.model.HandleEvent(ctx, path, eventUnmarshaler)
}

func (ap *aggregateProjectionModel) HandleEvent(ctx context.Context, path protoEvent.Path, eventUnmarshaler event.EventUnmarshaler) error {
	ap.lock.Lock()
	defer ap.lock.Unlock()
	switch {
	case ap.isFirstEvent:
		//we accept first event - it is snapshot or event with version 0
		ap.isFirstEvent = false
	case eventUnmarshaler.Version() <= ap.version:
		//ignore event - it was already applied
		return nil
	case eventUnmarshaler.Version() == ap.version+1:
		//apply event
	default:
		_, err := ap.eventstore.LoadFromVersion(ctx, path, ap.version+1, ap)
		if err != nil {
			return fmt.Errorf("cannot load previous events: %v", err)
		}
	}
	ap.version = eventUnmarshaler.Version()
	return ap.model.HandleEvent(ctx, path, eventUnmarshaler)
}

// Project load events from aggregates that below to path.
func (p *Projection) Project(path protoEvent.Path) error {
	paths := []protoEvent.Path{path}
	if path.AggregateId == "" {
		ps, err := p.eventstore.ListPaths(p.ctx, path)
		if err != nil {
			return fmt.Errorf("cannot load paths: %v", err)
		}
		paths = ps
	}

	var errors []error

	for _, path := range paths {
		agProj := eventstore.MakeProjection(path,
			p.numEventsInSnapshot,
			p.eventstore,
			func(ctx context.Context) (eventstore.Model, error) {
				p.lock.Lock()
				if aggp, ok := p.aggregateProjections[path2string(path)]; ok {
					p.lock.Unlock()
					return aggp, nil
				}
				p.lock.Unlock()

				model, err := p.factoryModel(ctx)
				if err != nil {
					return nil, err
				}
				return &aggregateProjectionModel{model: model, eventstore: p.eventstore, isFirstEvent: true}, nil
			})
		model, _, _, err := agProj.Project(p.ctx)
		if err != nil {
			errors = append(errors, err)
		}
		p.lock.Lock()
		defer p.lock.Unlock()
		p.aggregateProjections[path2string(path)] = model.(*aggregateProjectionModel)
	}
	if len(errors) > 0 {
		return fmt.Errorf("cannot load paths to projection: %v", errors)
	}
	return nil
}

// Forget forger projection for certain path. Path must point be full-filled.
func (p *Projection) Forget(path protoEvent.Path) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	ps := path2string(path)
	if _, ok := p.aggregateProjections[ps]; !ok {
		return fmt.Errorf("cannot remove non-exist projection")
	}
	delete(p.aggregateProjections, ps)
	return nil
}

type projectionHandler struct {
	projection *Projection
}

func (p projectionHandler) HandleEvent(ctx context.Context, path protoEvent.Path, eventUnmarshaler event.EventUnmarshaler) error {
	p.projection.lock.Lock()
	var ap *aggregateProjectionModel
	if found, ok := p.projection.aggregateProjections[path2string(path)]; ok {
		ap = found
	}
	p.projection.lock.Unlock()

	if ap != nil {
		return ap.HandleEvent(ctx, path, eventUnmarshaler)
	} else {
		return p.projection.Project(path)
	}
}

// SetTopicsToObserve set topics for observation for update events.
func (p *Projection) SetTopicsToObserve(topics []string) error {
	if p.subscriber == nil {
		return fmt.Errorf("projection doesn't support subscribe to topics")
	}

	var newObs eventbus.Observer
	if len(topics) > 0 {
		var err error
		newObs, err = p.subscriber.Subscribe(p.ctx, p.subscriptionId, topics, projectionHandler{p})
		if err != nil {
			return fmt.Errorf("cannot set topics to observe: %v", err)
		}
	}

	var oldObs eventbus.Observer
	p.lock.Lock()
	oldObs = p.observer
	p.observer = newObs
	p.lock.Unlock()
	if oldObs != nil {
		return oldObs.Cancel()
	}
	return nil
}

// Cancel cancel projection.
func (p *Projection) Cancel() error {
	p.cancel()
	return p.observer.Cancel()
}
