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
func NewProjection(ctx context.Context, eventstore eventstore.EventStore, numEventsInSnapshot int, subscriber eventbus.Subscriber, factoryModel FactoryModelFunc) (*Projection, error) {
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

type iterator struct {
	iter                  event.Iter
	num                   int
	version               uint64
	snapshotEventType     string
	hasSnapshot           bool
	needToLoadFromVersion bool
}

func (i *iterator) Next(e *event.EventUnmarshaler) bool {
	if i.iter.Next(e) {
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

func (ap *aggregateProjectionModel) HandleEventFromStore(ctx context.Context, path protoEvent.Path, iter event.Iter) (int, error) {
	i := iterator{
		version:     ap.version,
		hasSnapshot: ap.hasSnapshot,

		iter:              iter,
		snapshotEventType: ap.SnapshotEventType(),
	}
	err := ap.model.HandleEvent(ctx, path, &i)
	if err != nil {
		ap.version = i.version
		ap.hasSnapshot = i.hasSnapshot
	}
	return i.num, err
}

func (ap *aggregateProjectionModel) HandleEvent(ctx context.Context, path protoEvent.Path, iter event.Iter) error {
	ap.lock.Lock()
	defer ap.lock.Unlock()

	i := iterator{
		version:     ap.version,
		hasSnapshot: ap.hasSnapshot,

		iter:              iter,
		snapshotEventType: ap.SnapshotEventType(),
	}
	err := ap.model.HandleEvent(ctx, path, &i)
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

type projectPathsHandler struct {
	projection *Projection
}

func (pp projectPathsHandler) HandlePaths(ctx context.Context, iter eventstore.PathIter) error {
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
func (p *Projection) Project(path protoEvent.Path) error {
	pathsHandler := projectPathsHandler{projection: p}
	err := p.eventstore.ListPaths(p.ctx, path, &pathsHandler)
	if err != nil {
		return fmt.Errorf("cannot load paths: %v", err)
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
	if p.subscriber == nil {
		return fmt.Errorf("projection doesn't support subscribe to topics")
	}
	p.lock.Lock()
	if p.observer == nil {
		observer, err := p.subscriber.Subscribe(p.ctx, p.subscriptionId, topics, projectionHandler{p})
		if err != nil {
			p.lock.Unlock()
			return fmt.Errorf("projection cannot subscribe to topics: %v", err)
		}
		p.observer = observer
	}
	err := p.observer.SetTopics(p.ctx, topics)
	p.lock.Unlock()
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
