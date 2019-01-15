package eventbus

import (
	"context"

	"github.com/go-ocf/cqrs/event"
	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
	protoEventBus "github.com/go-ocf/cqrs/protobuf/eventbus"
)

// EventUnmarshaler provides functions over event that comes from observer.
type EventUnmarshaler struct {
	Event           protoEventBus.Event
	DataUnmarshaler event.UnmarshalerFunc
}

// Unmarshal unmarshal data to v where v is pointer to struct.
func (eu EventUnmarshaler) Unmarshal(v interface{}) error {
	return eu.DataUnmarshaler(eu.Event.Data, v)
}

// Version returns version of event.
func (eu EventUnmarshaler) Version() uint64 {
	return eu.Event.Version
}

// EventType returns event type of event for unmarshaling.
func (eu EventUnmarshaler) EventType() string {
	return eu.Event.EventType
}

// AggregateId returns aggregate id of event.
func (eu EventUnmarshaler) AggregateId() string {
	return eu.Event.Path.AggregateId
}

// Publisher publish events to topics
type Publisher interface {
	Publish(ctx context.Context, topics []string, path protoEvent.Path, event event.Event) error
}

// Subscriber creates observation over topics. When subscriptionID is same among more Subscribers events are balanced among them.
type Subscriber interface {
	Subscribe(ctx context.Context, subscriptionID string, topics []string, eh event.EventHandler) (Observer, error)
}

// Observer handles events from observation and forward them to event.EventHandler.
type Observer interface {
	Cancel() error
	Errors() <-chan error
}
