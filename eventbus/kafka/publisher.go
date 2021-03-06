package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/plgd-dev/cqrs/event"
	protoEventBus "github.com/plgd-dev/cqrs/protobuf/eventbus"

	sarama "github.com/Shopify/sarama"
)

// Publisher implements a eventbus.Publisher interface.
type Publisher struct {
	brokers []string
	config  *sarama.Config

	dataMarshalerFunc event.MarshalerFunc

	producer sarama.SyncProducer
}

// NewPublisher creates a publisher.
func NewPublisher(brokers []string, config *sarama.Config, eventMarshaler event.MarshalerFunc) (*Publisher, error) {
	if eventMarshaler == nil {
		return nil, errors.New("eventMarshaler is not set")
	}
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, err
	}
	return &Publisher{
		brokers:           brokers,
		config:            config,
		dataMarshalerFunc: eventMarshaler,
		producer:          producer,
	}, nil
}

// Publish publishes an event to topics.
func (b *Publisher) Publish(ctx context.Context, topics []string, groupId, aggregateId string, event event.Event) error {
	data, err := b.dataMarshalerFunc(event)
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
		_, _, err = b.producer.SendMessage(&sarama.ProducerMessage{
			Topic: t,
			Value: sarama.ByteEncoder(eData),
		})

		if err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		return fmt.Errorf("could not publish event: %v", errors)
	}

	return nil
}

// Close close publisher.
func (b *Publisher) Close() error {
	return b.producer.Close()
}
