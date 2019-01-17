package eventbus

import (
	"context"

	"github.com/go-ocf/cqrs/event"
	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
)

// Publisher publish event to topics
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
}
