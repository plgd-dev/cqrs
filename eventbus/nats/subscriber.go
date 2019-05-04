package nats

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventbus"
	protoEventBus "github.com/go-ocf/cqrs/protobuf/eventbus"
	nats "github.com/nats-io/go-nats"
)

// Subscriber implements a eventbus.Subscriber interface.
type Subscriber struct {
	clientId        string
	dataUnmarshaler event.UnmarshalerFunc
	errFunc         ErrFunc
	conn            *nats.Conn
	url             string
}

//Observer handles events from kafka
type Observer struct {
	lock            sync.Mutex
	dataUnmarshaler event.UnmarshalerFunc
	eventHandler    event.Handler
	errFunc         ErrFunc
	conn            *nats.Conn
	subscriptionId  string
	subs            map[string]*nats.Subscription
}

// ErrFunc used by observer to report error from observation
type ErrFunc func(err error)

// NewSubscriber creates a subscriber.
func NewSubscriber(url string, eventUnmarshaler event.UnmarshalerFunc, errFunc ErrFunc, options ...nats.Option) (*Subscriber, error) {
	conn, err := nats.Connect(url, options...)
	if err != nil {
		return nil, fmt.Errorf("cannot create client: %v", err)
	}

	return &Subscriber{
		dataUnmarshaler: eventUnmarshaler,
		errFunc:         errFunc,
		conn:            conn,
	}, nil
}

// Subscribe creates a observer that listen on events from topics.
func (b *Subscriber) Subscribe(ctx context.Context, subscriptionId string, topics []string, eh event.Handler) (eventbus.Observer, error) {
	observer := b.newObservation(ctx, subscriptionId, eh)

	err := observer.SetTopics(ctx, topics)
	if err != nil {
		return nil, fmt.Errorf("cannot subscribe: %v", err)
	}

	return observer, nil
}

// Close closes subscriber.
func (b *Subscriber) Close() {
	b.conn.Close()
}

func (b *Subscriber) newObservation(ctx context.Context, subscriptionId string, eh event.Handler) *Observer {
	return &Observer{
		conn:            b.conn,
		dataUnmarshaler: b.dataUnmarshaler,
		eventHandler:    eh,
		subscriptionId:  subscriptionId,
		subs:            make(map[string]*nats.Subscription),
	}
}

func (o *Observer) cleanUp(topics map[string]bool) (map[string]bool, error) {
	var errors []error
	for topic, sub := range o.subs {
		if _, ok := topics[topic]; !ok {
			err := sub.Unsubscribe()
			if err != nil {
				errors = append(errors, err)
			}
			delete(o.subs, topic)
		}
	}
	newSubs := make(map[string]bool)
	for topic := range topics {
		if _, ok := o.subs[topic]; !ok {
			newSubs[topic] = true
		}
	}

	if len(errors) > 0 {
		return nil, fmt.Errorf("cannot unsubscribe from topics: %v", errors)
	}
	return newSubs, nil
}

// SetTopics set new topics to observe.
func (o *Observer) SetTopics(ctx context.Context, topics []string) error {
	o.lock.Lock()
	defer o.lock.Unlock()

	mapTopics := make(map[string]bool)
	for _, topic := range topics {
		mapTopics[topic] = true
	}

	newTopicsForSub, err := o.cleanUp(mapTopics)
	if err != nil {
		return fmt.Errorf("cannot set topics: %v", err)
	}
	for topic := range newTopicsForSub {
		sub, err := o.conn.QueueSubscribe(topic, o.subscriptionId, o.handleMsg)
		if err != nil {
			o.cleanUp(make(map[string]bool))
			return fmt.Errorf("cannot subscribe to topics: %v", err)
		}
		o.subs[topic] = sub
	}

	return nil
}

// Close cancel observation and close connection to kafka.
func (o *Observer) Close() error {
	o.lock.Lock()
	defer o.lock.Unlock()
	_, err := o.cleanUp(make(map[string]bool))
	if err != nil {
		return fmt.Errorf("cannot close observer: %v", err)
	}
	return nil
}

func (o *Observer) handleMsg(msg *nats.Msg) {
	var e protoEventBus.Event

	err := e.Unmarshal(msg.Data)
	if err != nil {
		o.errFunc(fmt.Errorf("cannot unmarshal event: %v", err))
		return
	}

	i := iter{
		hasNext: true,
		e:       e,
		dataUnmarshaler: func(v interface{}) error {
			return o.dataUnmarshaler(e.Data, v)
		},
	}

	if err := o.eventHandler.Handle(context.Background(), &i); err != nil {
		o.errFunc(fmt.Errorf("cannot unmarshal event: %v", err))
	}
}

type iter struct {
	e               protoEventBus.Event
	dataUnmarshaler func(v interface{}) error
	hasNext         bool
}

func (i *iter) Next(ctx context.Context, e *event.EventUnmarshaler) bool {
	if i.hasNext {
		e.Version = i.e.Version
		e.AggregateId = i.e.AggregateId
		e.EventType = i.e.EventType
		e.GroupId = i.e.GroupId
		e.Unmarshal = i.dataUnmarshaler
		i.hasNext = false
		return true
	}
	return false
}

func (i *iter) Err() error {
	return nil
}
