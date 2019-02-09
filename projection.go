package cqrs

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventbus"
	"github.com/go-ocf/cqrs/eventstore"
)

// Model define interface of projectionModel.
type Model interface {
	eventstore.Model
}

// FactoryModelFunc creates user model.
type FactoryModelFunc func(ctx context.Context) (Model, error)

type aggregateProjectionModel struct {
	lock                sync.Mutex
	version             uint64
	hasSnapshot         bool
	model               Model
	numEventsInSnapshot int
	eventstore          eventstore.EventStore
}

// Projection project events to user defined model from evenstore and update it by events from subscriber.
type Projection struct {
	//immutable
	eventstore          eventstore.EventStore
	numEventsInSnapshot int
	ctx                 context.Context
	cancel              context.CancelFunc
	factoryModel        FactoryModelFunc

	subscriber     eventbus.Subscriber
	subscriptionId string

	//mutable part
	lock                 sync.Mutex
	observer             eventbus.Observer
	aggregateProjections map[string]*aggregateProjectionModel
}

// NewProjection creates projection.
func NewProjection(ctx context.Context, subscriptionId string, eventstore eventstore.EventStore, numEventsInSnapshot int, subscriber eventbus.Subscriber, factoryModel FactoryModelFunc) (*Projection, error) {
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
		subscriptionId:       subscriptionId,
	}

	return &rd, nil
}

func toString(groupId, aggregateIdstring string) string {
	return groupId + "." + aggregateIdstring
}

func (ap *aggregateProjectionModel) Handle(ctx context.Context, iter event.Iter) error {
	ap.lock.Lock()
	defer ap.lock.Unlock()
len
	i := iterator{
		version:     ap.version,
		hasSnapshot: ap.hasSnapshot,

		iter: iter,
	}
	err := ap.model.Handle(ctx, &i)
	if err != nil {
		return fmt.Errorf("cannot handle event to aggregate projection model: %v", err)
	}
	ap.version = i.version
	ap.hasSnapshot = i.hasSnapshot
	if i.needToLoadFromVersion {
		if i.version == 0 {
			_, err = ap.eventstore.LoadLatest(ctx, path, ap.numEventsInSnapshot, ap)
		} else {
			_, err = ap.eventstore.LoadFromVersion(ctx, path, ap.version+1, ap)
		}
		if err != nil {
			return fmt.Errorf("cannot load previous events: %v", err)
		}
	}
	return nil
}

type projectionHandler struct {
	projection *Projection
}

func (ph *projectionHandler) GetModel(ctx context.Context, apId string) (*aggregateProjectionModel, error) {
	var ok bool
	var apm *aggregateProjectionModel

	ph.projection.lock.Lock()
	defer ph.projection.lock.Unlock()
	if apm, ok = ph.projection.aggregateProjections[apId]; !ok {
		model, err := ph.projection.factoryModel(ctx)
		if err != nil {
			return nil, fmt.Errorf("cannot create model: %v", err)
		}
		apm = &aggregateProjectionModel{model: model, eventstore: ph.projection.eventstore}
	}
	return apm, nil
}

func (ph *projectionHandler) UpdateProjection(ctx context.Context, e *event.EventUnmarshaler) (ignore bool, reload bool, err error) {
	apm, err := GetModel(ctx, toStringe(e.GroupId, e.AggregateId))
	if err != nil {
		return false, false, fmt.Errorf("cannot update projection model: %v", err)
	}
	apm.lock.Lock()
	defer apm.lock.Unlock()
	switch {
	case !apm.hasSnapshot:
		//it is first event or snapshot accept it
		apm.hasSnapshot = true
		apm.Version = e.Version
	case apm.Version == e.version+1 && apm.hasSnapshot:
		apm.Version++
	case apm.Version <= e.version && apm.hasSnapshot:
		//ignore event - it was already applied
		return true, false, nil
	default:
		//need to reload
		return false, true, nil
	}
	return false, false, nil
}

type iterator struct {
	iter  event.Iter
	ph  *projectionHandler
	num                   int
	version               uint64
	snapshotEventType     string
	hasSnapshot           bool
	needToLoadFromVersion bool
	err error
}

func (i *iterator) Next(ctx context.Context, e *event.EventUnmarshaler) bool {
	for i.iter.Next(ctx, e) {
		ignore, reload, err = ph.UpdateProjection(ctx, e)
		if err != nil {
			i.err = err
			return false
		}
		if ignore {
			continue
		}


		if err != 
		switch {
		case e.Version == 0 || i.snapshotEventType == e.EventType:
			//it is first event or snapshot accept it
			i.hasSnapshot = true
		case e.Version == i.version+1 && i.hasSnapshot:
		case e.Version <= i.version && i.hasSnapshot:
			//ignore event - it was already applied
			return i.Next(e)
		default:
			i.needToLoadFromVersion = true
			return false
		}
		i.version = e.Version
		i.num++
		return true
	}
	return false
}

func (i *iterator) Err() error {
	return i.iter.Err()
}

func (ap *projectionHandler) Handle(ctx context.Context, iter event.Iter) error {
}


func (ap *projectionHandler) Handle(ctx context.Context, iter event.Iter) error {


	for iter.Next(&path) {
	apm, err := GetModel(ctx, iter)

	var path protoEvent.Path
	var errors []error
	for iter.Next(&path) {
		agProj := eventstore.MakeProjection(path,
			pp.projection.numEventsInSnapshot,
			pp.projection.eventstore,
			func(ctx context.Context) (eventstore.Model, error) {
				pp.projection.lock.Lock()
				if aggp, ok := pp.projection.aggregateProjections[path2string(path)]; ok {
					pp.projection.lock.Unlock()
					return aggp, nil
				}
				pp.projection.lock.Unlock()

				model, err := pp.projection.factoryModel(ctx)
				if err != nil {
					return nil, err
				}

				return &aggregateProjectionModel{model: model, eventstore: pp.projection.eventstore, numEventsInSnapshot: pp.projection.numEventsInSnapshot}, nil
			})
		model, _, _, err := agProj.Project(ctx)
		if err != nil {
			errors = append(errors, err)
		}
		pp.projection.lock.Lock()
		pp.projection.aggregateProjections[path2string(path)] = model.(*aggregateProjectionModel)
		pp.projection.lock.Unlock()
	}
	if len(errors) > 0 {
		return fmt.Errorf("%v", errors)
	}
	return iter.Err()
}

// Project load events from aggregates that below to path.
func (p *Projection) Project(query []eventstore.Query) error {
	pathsHandler := projectPathsHandler{projection: p}
	err := p.eventstore.ListPaths(p.ctx, path, &pathsHandler)
	if err != nil {
		return fmt.Errorf("cannot load paths: %v", err)
	}

	return nil
}

// Forget forger projection for certain path. Path must point be full-filled.
func (p *Projection) Forget(query []eventstore.Query) error {
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

func (p projectionHandler) HandleEvent(ctx context.Context, path protoEvent.Path, iter event.Iter) error {
	p.projection.lock.Lock()
	var ap *aggregateProjectionModel
	if found, ok := p.projection.aggregateProjections[path2string(path)]; ok {
		ap = found
	}
	p.projection.lock.Unlock()

	if ap != nil {
		return ap.HandleEvent(ctx, path, iter)
	}
	return p.projection.Project(path)
}

// SubscribeTo set topics for observation for update events.
func (p *Projection) SubscribeTo(topics []string) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.subscriber == nil {
		return fmt.Errorf("projection doesn't support subscribe to topics")
	}
	if p.observer == nil {
		observer, err := p.subscriber.Subscribe(p.ctx, p.subscriptionId, topics, projectionHandler{p})
		if err != nil {
			return fmt.Errorf("projection cannot subscribe to topics: %v", err)
		}
		p.observer = observer
	}
	err := p.observer.SetTopics(p.ctx, topics)
	if err != nil {
		return fmt.Errorf("projection cannot set topics: %v", err)
	}

	return nil
}

// Close cancel projection.
func (p *Projection) Close() error {
	p.cancel()
	return p.observer.Close()
}
