// Copyright (c) 2016 - The Event Horizon authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package eventbus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/go-ocf/cqrs/event"
	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
	"github.com/stretchr/testify/assert"
)

type mockEvent struct {
	VersionI     uint64
	EventTypeI   string
	AggregateIDI string
	Data         string
}

func (e mockEvent) Version() uint64 {
	return e.VersionI
}

func (e mockEvent) EventType() string {
	return e.EventTypeI
}

func (e mockEvent) AggregateId() string {
	return e.AggregateIDI
}

type mockEventHandler struct {
	newEvent chan mockEvent
}

func newMockEventHandler() *mockEventHandler {
	return &mockEventHandler{newEvent: make(chan mockEvent, 10)}
}

func (eh *mockEventHandler) HandleEvent(ctx context.Context, path protoEvent.Path, eu event.EventUnmarshaler) error {
	if eu.EventType() == "" {
		return errors.New("cannot determine type of event")
	}
	var e mockEvent
	err := eu.Unmarshal(&e)
	if err != nil {
		return err
	}
	eh.newEvent <- e
	return nil
}

func (eh *mockEventHandler) waitForEvent(timeout time.Duration) (mockEvent, error) {
	select {
	case e := <-eh.newEvent:
		return e, nil
	case <-time.After(timeout):
		return mockEvent{}, fmt.Errorf("timeout")
	}
}

func testWaitForAnyEvent(timeout time.Duration, eh1 *mockEventHandler, eh2 *mockEventHandler) (mockEvent, error) {
	select {
	case e := <-eh1.newEvent:
		return e, nil
	case e := <-eh2.newEvent:
		return e, nil
	case <-time.After(timeout):
		return mockEvent{}, fmt.Errorf("timeout")
	}
}

func testNewSubscription(t *testing.T, ctx context.Context, subscriber Subscriber, subscriptionId string, topics []string) (*mockEventHandler, Observer) {
	t.Log("Subscribe to testNewSubscription")
	m := newMockEventHandler()
	ob, err := subscriber.Subscribe(ctx, subscriptionId, topics, m)
	assert.NoError(t, err)
	assert.NotNil(t, ob)
	return m, ob
}

// AcceptanceTest is the acceptance test that all implementations of publisher, subscriber
// should pass. It should manually be called from a test case in each
// implementation:
//
//   func TestSubscriber(t *testing.T) {
//       ctx := context.Background() // Or other when testing namespaces.
//       publisher := NewPublisher()
//       subscriber := NewSubscriber()
//       timeout := time.Second*5
//       topics := []string{"a", "b"}
//       eventbus.AcceptanceTest(t, ctx, timeout, topics, publisher, subscriber)
//   }
//
func AcceptanceTest(t *testing.T, ctx context.Context, timeout time.Duration, topics []string, publisher Publisher, subscriber Subscriber) {
	//savedEvents := []Event{}
	AggregateID1 := "aggregateID1"
	AggregateID2 := "aggregateID2"
	aggregateID1Path := protoEvent.Path{
		AggregateId: AggregateID1,
		Path:        []string{"deviceId"},
	}
	/*
		aggregateID2Path := protoEvent.Path{
			AggregateId: AggregateID2,
			Path:        []string{"deviceId"},
		}
		aggregateIDNotExistPath := protoEvent.Path{
			AggregateId: "notExist",
			Path:        []string{"deviceId"},
		}
	*/
	eventsToPublish := []mockEvent{
		mockEvent{
			EventTypeI:   "test0",
			AggregateIDI: AggregateID1,
		},
		mockEvent{
			VersionI:     1,
			EventTypeI:   "test1",
			AggregateIDI: AggregateID1,
		},
		mockEvent{
			VersionI:     2,
			EventTypeI:   "test2",
			AggregateIDI: AggregateID1,
		},
		mockEvent{
			VersionI:     3,
			EventTypeI:   "test3",
			AggregateIDI: AggregateID1,
		},
		mockEvent{
			VersionI:     4,
			EventTypeI:   "test4",
			AggregateIDI: AggregateID1,
		},
		mockEvent{
			VersionI:     5,
			EventTypeI:   "test5",
			AggregateIDI: AggregateID1,
		},
		mockEvent{
			VersionI:     6,
			EventTypeI:   "test6",
			AggregateIDI: AggregateID2,
		},
	}

	assert.Equal(t, 2, len(topics))

	t.Log("Without subscription")
	err := publisher.Publish(ctx, topics[0:1], aggregateID1Path, eventsToPublish[0])
	assert.NoError(t, err)

	// Add handlers and observers.
	t.Log("Subscribe to first topic")
	m0, ob0 := testNewSubscription(t, ctx, subscriber, "0", topics[0:1])

	err = publisher.Publish(ctx, topics[0:1], aggregateID1Path, eventsToPublish[1])
	assert.NoError(t, err)

	event, err := m0.waitForEvent(timeout)
	assert.NoError(t, err)
	assert.Equal(t, eventsToPublish[1], event)

	err = ob0.Cancel()
	assert.NoError(t, err)
	t.Log("Subscribe more observers")
	m1, ob1 := testNewSubscription(t, ctx, subscriber, "1", topics[1:2])
	defer func() {
		err = ob1.Cancel()
		assert.NoError(t, err)
		for err := range ob1.Errors() {
			assert.NoError(t, err)
		}
	}()
	m2, ob2 := testNewSubscription(t, ctx, subscriber, "2", topics[1:2])
	defer func() {
		err = ob2.Cancel()
		assert.NoError(t, err)
		for err := range ob2.Errors() {
			assert.NoError(t, err)
		}
	}()
	m3, ob3 := testNewSubscription(t, ctx, subscriber, "shared", topics[0:1])
	defer func() {
		err = ob3.Cancel()
		assert.NoError(t, err)
		for err := range ob3.Errors() {
			assert.NoError(t, err)
		}
	}()
	m4, ob4 := testNewSubscription(t, ctx, subscriber, "shared", topics[0:1])
	defer func() {
		err = ob4.Cancel()
		assert.NoError(t, err)
		for err := range ob4.Errors() {
			assert.NoError(t, err)
		}
	}()

	time.Sleep(time.Second * 5)

	err = publisher.Publish(ctx, topics, aggregateID1Path, eventsToPublish[2])
	assert.NoError(t, err)

	event, err = m1.waitForEvent(timeout)
	assert.NoError(t, err)
	assert.Equal(t, eventsToPublish[2], event)

	event, err = m2.waitForEvent(timeout)
	assert.NoError(t, err)
	assert.Equal(t, eventsToPublish[2], event)

	event, err = testWaitForAnyEvent(timeout, m3, m4)
	assert.NoError(t, err)
	assert.Equal(t, eventsToPublish[2], event)
}
