package cqrs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	resources "github.com/go-ocf/resource-aggregate/protobuf"
	"github.com/go-ocf/resource-aggregate/protobuf/commands"
	"github.com/gofrs/uuid"
	"github.com/panjf2000/ants"

	"github.com/go-ocf/cqrs/eventbus/kafka"
	"github.com/go-ocf/cqrs/eventstore"
	"github.com/go-ocf/cqrs/eventstore/mongodb"
	"github.com/go-ocf/resource-aggregate/protobuf/events"
	"github.com/stretchr/testify/assert"
)

func TestProjection(t *testing.T) {
	numEventsInSnapshot := 1
	waitForSubscription := time.Second * 5

	// Connect to localhost if not running inside docker
	broker := os.Getenv("KAFKA_EMULATOR_BOOTSTRAP_SERVER")
	if broker == "" {
		broker = "localhost:9092"
	}

	topics := []string{"test_projection_topic0_" + uuid.Must(uuid.NewV4()).String(), "test_projection_topic1_" + uuid.Must(uuid.NewV4()).String()}

	config := sarama.NewConfig()
	config.Producer.Flush.MaxMessages = 1

	publisher, err := kafka.NewPublisher(
		[]string{broker},
		config,
		json.Marshal)
	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	subscriber, err := kafka.NewSubscriber(
		[]string{broker},
		config,
		json.Unmarshal,
		func(err error) { assert.NoError(t, err) },
	)
	assert.NoError(t, err)
	assert.NotNil(t, subscriber)

	// Local Mongo testing with Docker
	url := os.Getenv("MONGO_HOST")

	if url == "" {
		// Default to localhost
		url = "localhost:27017"
	}

	pool, err := ants.NewPool(16)
	assert.NoError(t, err)
	defer pool.Release()

	store, err := mongodb.NewEventStore(url, "test_projection", "events", 128, pool, func(v interface{}) ([]byte, error) {
		if p, ok := v.(ProtobufMarshaler); ok {
			return p.Marshal()
		}
		return nil, fmt.Errorf("marshal is not supported by %T", v)
	}, func(b []byte, v interface{}) error {
		if p, ok := v.(ProtobufUnmarshaler); ok {
			return p.Unmarshal(b)
		}
		return fmt.Errorf("marshal is not supported by %T", v)
	})
	/*bson.Marshal, bson.Unmarshal*/
	assert.NoError(t, err)
	assert.NotNil(t, store)

	ctx := context.Background()

	defer store.Close()
	defer func() {
		err = store.Clear(ctx)
		assert.NoError(t, err)
	}()

	type Path struct {
		GroupId     string
		AggregateId string
	}

	path1 := Path{
		GroupId:     "1",
		AggregateId: "ID1",
	}

	path2 := Path{
		GroupId:     "1",
		AggregateId: "ID2",
	}

	path3 := Path{
		GroupId:     "1",
		AggregateId: "ID3",
	}

	commandPub1 := commands.PublishResourceRequest{
		ResourceId: path1.AggregateId,
		Resource: &resources.Resource{
			Id: path1.AggregateId,
		},
		AuthorizationContext: &commands.AuthorizationContext{},
	}

	commandPub2 := commands.PublishResourceRequest{
		ResourceId: path2.AggregateId,
		Resource: &resources.Resource{
			Id: path2.AggregateId,
		},
		AuthorizationContext: &commands.AuthorizationContext{},
	}

	commandPub3 := commands.PublishResourceRequest{
		ResourceId: path3.AggregateId,
		Resource: &resources.Resource{
			Id: path3.AggregateId,
		},
		AuthorizationContext: &commands.AuthorizationContext{},
	}

	commandUnpub1 := commands.UnpublishResourceRequest{
		ResourceId:           path1.AggregateId,
		AuthorizationContext: &commands.AuthorizationContext{},
	}

	commandUnpub3 := commands.UnpublishResourceRequest{
		ResourceId:           path3.AggregateId,
		AuthorizationContext: &commands.AuthorizationContext{},
	}

	/*
		path2topics := func(path Path, event event.Event) []string {
			return topics
		}
	*/

	a1, err := NewAggregate(path1.GroupId, path1.AggregateId, 1, numEventsInSnapshot, store, func(context.Context) (AggregateModel, error) {
		return &ResourceStateSnapshotTaken{events.ResourceStateSnapshotTaken{Id: path1.AggregateId, Resource: &resources.Resource{}, EventMetadata: &resources.EventMetadata{}}}, nil
	})
	assert.NoError(t, err)

	evs, err := a1.HandleCommand(ctx, commandPub1)
	assert.NoError(t, err)
	assert.NotNil(t, evs)

	snapshotEventType := func() string {
		s := &ResourceStateSnapshotTaken{}
		return s.SnapshotEventType()
	}

	a2, err := NewAggregate(path2.GroupId, path2.AggregateId, 1, numEventsInSnapshot, store, func(context.Context) (AggregateModel, error) {
		return &ResourceStateSnapshotTaken{events.ResourceStateSnapshotTaken{Id: path2.AggregateId, Resource: &resources.Resource{}, EventMetadata: &resources.EventMetadata{}}}, nil
	})
	assert.NoError(t, err)

	evs, err = a2.HandleCommand(ctx, commandPub2)
	assert.NoError(t, err)
	assert.NotNil(t, evs)

	projection, err := NewProjection(ctx, store, "testProjection", subscriber, func(context.Context) (eventstore.Model, error) { return &mockEventHandler{}, nil })
	assert.NoError(t, err)

	err = projection.Project(ctx, []eventstore.QueryFromSnapshot{
		eventstore.QueryFromSnapshot{
			GroupId:           path1.GroupId,
			AggregateId:       path1.AggregateId,
			SnapshotEventType: snapshotEventType(),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(projection.Models(nil)))

	err = projection.Project(ctx, []eventstore.QueryFromSnapshot{
		eventstore.QueryFromSnapshot{
			GroupId:           path2.GroupId,
			AggregateId:       path2.AggregateId,
			SnapshotEventType: snapshotEventType(),
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(projection.Models(nil)))

	err = projection.SubscribeTo(topics)
	assert.NoError(t, err)

	time.Sleep(waitForSubscription)

	a3, err := NewAggregate(path3.GroupId, path3.AggregateId, 1, numEventsInSnapshot, store, func(context.Context) (AggregateModel, error) {
		return &ResourceStateSnapshotTaken{events.ResourceStateSnapshotTaken{Id: path3.AggregateId, Resource: &resources.Resource{}, EventMetadata: &resources.EventMetadata{}}}, nil
	})
	assert.NoError(t, err)

	evs, err = a3.HandleCommand(ctx, commandPub3)
	assert.NoError(t, err)
	assert.NotNil(t, evs)
	for _, e := range evs {
		err = publisher.Publish(ctx, topics, path3.GroupId, path3.AggregateId, e)
		assert.NoError(t, err)
	}

	assert.Equal(t, 3, len(projection.Models(nil)))

	evs, err = a1.HandleCommand(ctx, commandUnpub1)
	assert.NoError(t, err)
	assert.NotNil(t, evs)
	for _, e := range evs {
		err = publisher.Publish(ctx, topics, path3.GroupId, path3.AggregateId, e)
		assert.NoError(t, err)
	}

	err = projection.SubscribeTo(topics[0:1])
	assert.NoError(t, err)

	time.Sleep(waitForSubscription)

	err = projection.Forget([]eventstore.QueryFromSnapshot{
		eventstore.QueryFromSnapshot{
			GroupId:           path3.GroupId,
			AggregateId:       path3.AggregateId,
			SnapshotEventType: snapshotEventType(),
		},
	})
	assert.NoError(t, err)

	evs, err = a3.HandleCommand(ctx, commandUnpub3)
	assert.NoError(t, err)
	assert.NotNil(t, evs)
	for _, e := range evs {
		err = publisher.Publish(ctx, topics[1:], path3.GroupId, path3.AggregateId, e)
		assert.NoError(t, err)
	}

	time.Sleep(time.Second)
	projection.lock.Lock()
	assert.Equal(t, 2, len(projection.Models(nil)))
	projection.lock.Unlock()

	err = projection.SubscribeTo(nil)
	assert.NoError(t, err)

	time.Sleep(waitForSubscription)

	evs, err = a1.HandleCommand(ctx, commandPub1)
	assert.NoError(t, err)
	assert.NotNil(t, evs)
	for _, e := range evs {
		err = publisher.Publish(ctx, topics, path1.GroupId, path1.AggregateId, e)
		assert.NoError(t, err)
	}

	projection.lock.Lock()
	assert.Equal(t, 2, len(projection.Models(nil)))
	projection.lock.Unlock()
}
