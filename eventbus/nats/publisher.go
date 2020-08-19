package nats

import (
	"context"
	"errors"
	"fmt"

	"github.com/plgd-dev/cqrs/event"
	protoEventBus "github.com/plgd-dev/cqrs/protobuf/eventbus"
	nats "github.com/nats-io/nats.go"
)

// Publisher implements a eventbus.Publisher interface.
type Publisher struct {
	dataMarshaler event.MarshalerFunc
	conn          *nats.Conn
}

// NewPublisher creates a publisher.
func NewPublisher(url string, eventMarshaler event.MarshalerFunc, options ...nats.Option) (*Publisher, error) {
	conn, err := nats.Connect(url, options...)
	if err != nil {
		return nil, fmt.Errorf("cannot connect to server: %w", err)
	}

	return &Publisher{
		dataMarshaler: eventMarshaler,
		conn:          conn,
	}, nil
}

// Publish publishes an event to topics.
func (p *Publisher) Publish(ctx context.Context, topics []string, groupId, aggregateId string, event event.Event) error {
	data, err := p.dataMarshaler(event)
	if err != nil {
		return errors.New("could not marshal data for event: " + err.Error())
	}

	e := protoEventBus.Event{
		EventType:   event.EventType(),
		Data:        data,
		Version:     event.Version(),
		GroupId:     groupId,
		AggregateId: aggregateId,
	}

	eData, err := e.Marshal()
	if err != nil {
		return errors.New("could not marshal event: " + err.Error())
	}

	var errors []error
	for _, t := range topics {
		err := p.conn.Publish(t, eData)
		if err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		return fmt.Errorf("cannot publish events: %v", errors)
	}

	return nil
}

// Close close connection to nats
func (p *Publisher) Close() {
	p.conn.Close()
}
