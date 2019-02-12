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

// Projection project events to user defined model from evenstore and update it by events from subscriber.
type Projection struct {
	//immutable
	projection *eventstore.Projection
	ctx        context.Context
	cancel     context.CancelFunc

	subscriber     eventbus.Subscriber
	subscriptionId string

	//mutable part
	lock     sync.Mutex
	observer eventbus.Observer
}

// NewProjection creates projection.
func NewProjection(ctx context.Context, store eventstore.EventStore, subscriptionId string, subscriber eventbus.Subscriber, factoryModel eventstore.FactoryModelFunc) (*Projection, error) {
	if store == nil {
		return nil, errors.New("invalid handle of event store")
	}

	projCtx, projCancel := context.WithCancel(ctx)

	rd := Projection{
		projection:     eventstore.NewProjection(store, factoryModel),
		ctx:            projCtx,
		cancel:         projCancel,
		subscriber:     subscriber,
		subscriptionId: subscriptionId,
	}

	return &rd, nil
}

// Project load events from aggregates that below to path.
func (p *Projection) Project(ctx context.Context, query []eventstore.QueryFromSnapshot) error {
	return p.projection.Project(ctx, query)
}

// Forget projection for certain query.
func (p *Projection) Forget(query []eventstore.QueryFromSnapshot) error {
	return p.projection.Forget(query)
}

// Handle events to projection. This events comes from eventbus and it can trigger reload on eventstore.
func (p *Projection) Handle(ctx context.Context, iter event.Iter) error {
	return p.projection.HandleWithReload(ctx, iter)
}

// SubscribeTo set topics for observation for update events.
func (p *Projection) SubscribeTo(topics []string) error {
	p.lock.Lock()
	defer p.lock.Unlock()

	if p.observer == nil {
		if p.subscriber == nil {
			return fmt.Errorf("projection doesn't support subscribe to topics")
		}
		observer, err := p.subscriber.Subscribe(p.ctx, p.subscriptionId, topics, p)
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

// Models get models from projection
func (p *Projection) Models(queries []eventstore.QueryFromSnapshot) []eventstore.Model {
	return p.projection.Models(queries)
}

// Close cancel projection.
func (p *Projection) Close() error {
	p.cancel()
	return p.observer.Close()
}
