/**
 * Copyright (c) 2018 KISTLER INSTRUMENTE AG, Winterthur, Switzerland
 *
 * @file eventbus.go
 *
 * Created on: Sep 21, 2018
 * Project: eh-kafka
 * Description: Event Bus Kafka for Event Horizont
 *
 * This file is part of eh-kafka.
 *
 * eh-kafka is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 2.1 of the License, or
 * (at your option) any later version.
 *
 * eh-kafka is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with eh-kafka. If not, see <http://www.gnu.org/licenses/>.
 */

package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-ocf/cqrs/event"
	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
	protoEventBus "github.com/go-ocf/cqrs/protobuf/eventbus"

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
func (b *Publisher) Publish(ctx context.Context, topics []string, path protoEvent.Path, event event.Event) error {
	data, err := b.dataMarshalerFunc(event)
	if err != nil {
		return errors.New("could not marshal data for event: " + err.Error())
	}

	e := protoEventBus.Event{
		EventType: event.EventType(),
		Data:      data,
		Version:   event.Version(),
		Path:      &path,
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
