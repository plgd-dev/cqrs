package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventbus"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func TestSubscriber(t *testing.T) {
	// Connect to localhost if not running inside docker
	broker := os.Getenv("KAFKA_EMULATOR_BOOTSTRAP_SERVER")
	if broker == "" {
		broker = "localhost:9092"
	}

	topics := []string{"test_kafka_topic0_" + uuid.Must(uuid.NewV4()).String(), "test_kafka_topic1" + uuid.Must(uuid.NewV4()).String()}

	config := sarama.NewConfig()
	config.Producer.Flush.MaxMessages = 1

	timeout := time.Second * 30

	publisher, err := NewPublisher(
		[]string{broker},
		config,
		json.Marshal)
	assert.NoError(t, err)
	assert.NotNil(t, publisher)
	defer publisher.Close()

	subscriber, err := NewSubscriber(
		[]string{broker},
		config,
		json.Unmarshal,
		func(f func()) error { go f(); return nil },
		func(err error) { assert.NoError(t, err) },
	)
	assert.NotNil(t, subscriber)

	acceptanceTest(t, context.Background(), timeout, topics, publisher, subscriber)
}

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

func (eh *mockEventHandler) Handle(ctx context.Context, iter event.Iter) error {
	var eu event.EventUnmarshaler

	for iter.Next(ctx, &eu) {
		if eu.EventType == "" {
			return errors.New("cannot determine type of event")
		}
		var e mockEvent
		err := eu.Unmarshal(&e)
		if err != nil {
			return err
		}
		eh.newEvent <- e
	}

	return iter.Err()
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

func testNewSubscription(t *testing.T, ctx context.Context, subscriber eventbus.Subscriber, subscriptionId string, topics []string) (*mockEventHandler, eventbus.Observer) {
	t.Log("Subscribe to testNewSubscription")
	m := newMockEventHandler()
	ob, err := subscriber.Subscribe(ctx, subscriptionId, topics, m)
	assert.NoError(t, err)
	assert.NotNil(t, ob)
	if ob == nil {
		return nil, nil
	}
	return m, ob
}

func acceptanceTest(t *testing.T, ctx context.Context, timeout time.Duration, topics []string, publisher eventbus.Publisher, subscriber eventbus.Subscriber) {
	AggregateID1 := "aggregateID1"
	AggregateID2 := "aggregateID2"
	type Path struct {
		AggregateId string
		GroupId     string
	}

	aggregateID1Path := Path{
		AggregateId: AggregateID1,
		GroupId:     "deviceId",
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
	err := publisher.Publish(ctx, topics[0:1], aggregateID1Path.GroupId, aggregateID1Path.AggregateId, eventsToPublish[0])
	assert.NoError(t, err)

	// Add handlers and observers.
	t.Log("Subscribe to first topic")
	m0, ob0 := testNewSubscription(t, ctx, subscriber, "sub-0", topics[0:1])

	err = publisher.Publish(ctx, topics[0:1], aggregateID1Path.GroupId, aggregateID1Path.AggregateId, eventsToPublish[1])
	assert.NoError(t, err)

	event, err := m0.waitForEvent(timeout)
	assert.NoError(t, err)

	assert.Equal(t, eventsToPublish[0], event)

	event, err = m0.waitForEvent(timeout)
	assert.NoError(t, err)
	assert.Equal(t, eventsToPublish[1], event)

	err = ob0.Close()
	assert.NoError(t, err)

	t.Log("Subscribe more observers")
	m1, ob1 := testNewSubscription(t, ctx, subscriber, "sub-1", topics[1:2])
	defer func() {
		err = ob1.Close()
		assert.NoError(t, err)
	}()
	m2, ob2 := testNewSubscription(t, ctx, subscriber, "sub-2", topics[1:2])
	defer func() {
		err = ob2.Close()
		assert.NoError(t, err)
	}()
	m3, ob3 := testNewSubscription(t, ctx, subscriber, "sub-shared", topics[0:1])
	defer func() {
		err = ob3.Close()
		assert.NoError(t, err)
	}()
	m4, ob4 := testNewSubscription(t, ctx, subscriber, "sub-shared", topics[0:1])
	defer func() {
		err = ob4.Close()
		assert.NoError(t, err)
	}()

	err = publisher.Publish(ctx, topics, aggregateID1Path.GroupId, aggregateID1Path.AggregateId, eventsToPublish[2])
	assert.NoError(t, err)

	event1, err := m1.waitForEvent(timeout)
	assert.NoError(t, err)
	assert.Equal(t, eventsToPublish[2], event1)

	event2, err := m2.waitForEvent(timeout)
	assert.NoError(t, err)
	assert.Equal(t, eventsToPublish[2], event2)

	event3, err := testWaitForAnyEvent(timeout, m3, m4)
	assert.NoError(t, err)
	assert.Equal(t, eventsToPublish[0], event3)

	event3, err = testWaitForAnyEvent(timeout, m3, m4)
	assert.NoError(t, err)
	assert.Equal(t, eventsToPublish[1], event3)

	event3, err = testWaitForAnyEvent(timeout, m3, m4)
	assert.NoError(t, err)
	assert.Equal(t, eventsToPublish[2], event3)

	topic := "new_topic_" + uuid.Must(uuid.NewV4()).String()
	topics = append(topics, topic)
	err = ob4.SetTopics(ctx, topics)
	assert.NoError(t, err)

	err = publisher.Publish(ctx, []string{topic}, aggregateID1Path.GroupId, aggregateID1Path.AggregateId, eventsToPublish[3])
	assert.NoError(t, err)

	event4, err := m4.waitForEvent(timeout)
	assert.NoError(t, err)
	assert.Equal(t, eventsToPublish[2], event4)

	event4, err = m4.waitForEvent(timeout)
	assert.NoError(t, err)
	assert.Equal(t, eventsToPublish[3], event4)

	err = ob4.SetTopics(ctx, nil)
	assert.NoError(t, err)
}
