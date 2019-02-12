package eventstore

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-ocf/cqrs/event"
)

// Model user defined model where events from eventstore will be projected.
type Model interface {
	event.Handler
	SnapshotEventType() string
}

// FactoryModelFunc creates user model.
type FactoryModelFunc func(ctx context.Context) (Model, error)

type aggregateModel struct {
	groupId     string
	aggregateId string
	model       Model
	version     uint64
	hasSnapshot bool
	lock        sync.Mutex
}

func (am *aggregateModel) SnapshotEventType() string {
	return am.model.SnapshotEventType()
}

func (am *aggregateModel) Update(e *event.EventUnmarshaler) (ignore bool, reload bool) {
	am.lock.Lock()
	defer am.lock.Unlock()

	switch {
	case e.Version == 0 || am.SnapshotEventType() == e.EventType:
		am.version = e.Version
		am.hasSnapshot = true
	case am.version+1 == e.Version && am.hasSnapshot:
		am.version = e.Version
	case am.version >= e.Version:
		//ignore event - it was already applied
		return true, false
	default:
		//need to reload
		return false, true
	}
	return false, false

}

func (am *aggregateModel) Handle(ctx context.Context, iter event.Iter) error {
	return am.model.Handle(ctx, iter)
}

// Projection projects events from eventstore to user model.
type Projection struct {
	store EventStore

	factoryModel    FactoryModelFunc
	lock            sync.Mutex
	aggregateModels map[string]map[string]*aggregateModel
}

// NewProjection projection over eventstore.
func NewProjection(store EventStore, factoryModel FactoryModelFunc) *Projection {
	return &Projection{
		store:           store,
		factoryModel:    factoryModel,
		aggregateModels: make(map[string]map[string]*aggregateModel),
	}
}

type iterator struct {
	iter       event.Iter
	firstEvent *event.EventUnmarshaler
	model      *aggregateModel

	nextEventToProcess *event.EventUnmarshaler
	err                error
	reload             *QueryFromVersion
}

func (i *iterator) Rewind(ctx context.Context) {
	var e event.EventUnmarshaler
	for i.iter.Next(ctx, &e) {
		if e.GroupId != i.model.groupId || e.AggregateId != i.model.aggregateId {
			i.nextEventToProcess = &e
			return
		}
	}
}

func (i *iterator) RewindIgnore(ctx context.Context, e *event.EventUnmarshaler) bool {
	for i.iter.Next(ctx, e) {
		if e.GroupId != i.model.groupId || e.AggregateId != i.model.aggregateId {
			i.nextEventToProcess = e
			return false
		}
		ignore, _ := i.model.Update(e)
		if !ignore {
			return true
		}
	}
	return false
}

func (i *iterator) Next(ctx context.Context, e *event.EventUnmarshaler) bool {
	if i.firstEvent != nil {
		tmp := i.firstEvent
		i.firstEvent = nil
		ignore, reload := i.model.Update(tmp)
		if reload {
			i.reload = &QueryFromVersion{AggregateId: e.AggregateId, Version: i.model.version}
			i.Rewind(ctx)
			return false
		}
		if ignore {
			if i.RewindIgnore(ctx, e) {
				return true
			}
		}
		*e = *tmp
		return true
	}

	if i.RewindIgnore(ctx, e) {
		return true
	}
	return false
}

func (i *iterator) Err() error {
	return i.iter.Err()
}

func (p *Projection) getModel(ctx context.Context, groupId, aggregateId string) (*aggregateModel, error) {
	var ok bool
	var mapApm map[string]*aggregateModel
	var apm *aggregateModel

	p.lock.Lock()
	defer p.lock.Unlock()
	if mapApm, ok = p.aggregateModels[groupId]; !ok {
		mapApm = make(map[string]*aggregateModel)
		p.aggregateModels[groupId] = mapApm
	}
	if apm, ok = mapApm[aggregateId]; !ok {
		model, err := p.factoryModel(ctx)
		if err != nil {
			return nil, fmt.Errorf("cannot create model: %v", err)
		}
		apm = &aggregateModel{groupId: groupId, aggregateId: aggregateId, model: model}
		mapApm[aggregateId] = apm
	}
	return apm, nil
}

func (p *Projection) handle(ctx context.Context, iter event.Iter) (reloadQueries []QueryFromVersion, err error) {
	var e event.EventUnmarshaler
	if !iter.Next(ctx, &e) {
		return nil, iter.Err()
	}
	ie := &e
	reloadQueries = make([]QueryFromVersion, 0, 32)
	for ie != nil {
		am, err := p.getModel(ctx, ie.GroupId, ie.AggregateId)
		if err != nil {
			return nil, fmt.Errorf("cannot handle projection: %v", err)
		}
		i := iterator{
			iter:               iter,
			firstEvent:         ie,
			model:              am,
			nextEventToProcess: nil,
			err:                nil,
			reload:             nil,
		}
		err = am.Handle(ctx, &i)
		if err != nil {
			return nil, fmt.Errorf("cannot handle projection: %v", err)
		}
		//check if we are on the end
		if i.nextEventToProcess == nil {
			if i.Next(ctx, &e) {
				//iterator need to mode to next
				i.Rewind(ctx)
			}
		}

		ie = i.nextEventToProcess

		if i.reload != nil {
			reloadQueries = append(reloadQueries, *i.reload)
		}
	}

	return nil, nil
}

// Handle update projection by events.
func (p *Projection) Handle(ctx context.Context, iter event.Iter) error {
	_, err := p.handle(ctx, iter)
	return err
}

// Handle update projection by events.
func (p *Projection) HandleWithReload(ctx context.Context, iter event.Iter) error {
	//reload queries for db because version of events was greater > lastVersionSeen+1
	reloadQueries, err := p.handle(ctx, iter)
	if err != nil {
		return fmt.Errorf("cannot handle events with reload: %v", err)
	}

	if len(reloadQueries) > 0 {
		err := p.store.LoadFromVersion(ctx, reloadQueries, p)
		if err != nil {
			return fmt.Errorf("cannot reload events for db: %v", err)
		}
	}
	return nil
}

// Project update projection from snapshots defined by query. Verson in Query is ignored.
func (p *Projection) Project(ctx context.Context, queries []QueryFromSnapshot) (err error) {
	return p.store.LoadFromSnapshot(ctx, queries, p)
}

// Forget drop projection by query.Verson in Query is ignored.
func (p *Projection) Forget(queries []QueryFromSnapshot) (err error) {
	p.lock.Lock()
	defer p.lock.Unlock()
	for _, query := range queries {
		if query.AggregateId == "" {
			delete(p.aggregateModels, query.GroupId)
		} else {
			if m, ok := p.aggregateModels[query.GroupId]; ok {
				delete(m, query.AggregateId)
				if len(m) == 0 {
					delete(p.aggregateModels, query.GroupId)
				}
			}
		}
	}

	return nil
}

func makeModelId(groupId, aggregateId string) string {
	return groupId + "." + aggregateId
}

func (p *Projection) allModels(models map[string]Model) map[string]Model {
	for groupId, group := range p.aggregateModels {
		for aggrId, apm := range group {
			models[makeModelId(groupId, aggrId)] = apm.model
		}
	}
	return models
}

func (p *Projection) models(queries []QueryFromSnapshot) map[string]Model {
	models := make(map[string]Model)
	p.lock.Lock()
	defer p.lock.Unlock()

	if len(queries) == 0 {
		return p.allModels(models)
	}
	for _, query := range queries {
		switch {
		case query.GroupId == "" && query.AggregateId == "":
			return p.allModels(models)
		case query.GroupId != "" && query.AggregateId == "":
			if aggregates, ok := p.aggregateModels[query.GroupId]; ok {
				for aggrId, apm := range aggregates {
					models[makeModelId(query.GroupId, aggrId)] = apm.model
				}
			}
		default:
			if aggregates, ok := p.aggregateModels[query.GroupId]; ok {
				if apm, ok := aggregates[query.AggregateId]; ok {
					models[makeModelId(query.GroupId, query.AggregateId)] = apm.model
				}
			}
		}
	}

	return models
}

// Models return models from projection.
func (p *Projection) Models(queries []QueryFromSnapshot) []Model {
	models := p.models(queries)
	result := make([]Model, 0, len(models))
	for _, m := range models {
		result = append(result, m)
	}
	return result
}
