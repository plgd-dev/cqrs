package cqrs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-ocf/resources/protobuf/resources"
	"github.com/go-ocf/resources/protobuf/resources/commands"
	"github.com/gofrs/uuid"

	"github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventbus/kafka"
	"github.com/go-ocf/cqrs/eventstore"
	"github.com/go-ocf/cqrs/eventstore/mongodb"
	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
	"github.com/go-ocf/resources/protobuf/resources/events"
	"github.com/stretchr/testify/assert"
)

func TestProjection(t *testing.T) {
	numEventsInSnapshot := 1

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

	subscriber := kafka.NewSubscriber(
		[]string{broker},
		config,
		json.Unmarshal)
	assert.NotNil(t, subscriber)

	// Local Mongo testing with Docker
	url := os.Getenv("MONGO_HOST")

	if url == "" {
		// Default to localhost
		url = "localhost:27017"
	}

	store, err := mongodb.NewEventStore(url, "test_projection", "events", func(v interface{}) ([]byte, error) {
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

	path1 := protoEvent.Path{
		Path:        []string{"1", "2", "3", "4"},
		AggregateId: "ID1",
	}

	path2 := protoEvent.Path{
		Path:        []string{"1", "2", "3", "4"},
		AggregateId: "ID2",
	}

	path3 := protoEvent.Path{
		Path:        []string{"1", "2", "3", "4"},
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

	path2topic1 := func(path protoEvent.Path, event event.Event) []string {
		return topics[0:1]
	}
	path2topic2 := func(path protoEvent.Path, event event.Event) []string {
		return topics[1:2]
	}
	/*
		path2topics := func(path protoEvent.Path, event event.Event) []string {
			return topics
		}
	*/

	a1, err := MakeAggregate(path1, numEventsInSnapshot, store, publisher, path2topic1, func(context.Context) (AggregateModel, error) {
		return &ResourceStateSnapshotTaken{events.ResourceStateSnapshotTaken{Id: path1.AggregateId, ResourceState: &resources.ResourceState{}, EventMetadata: &resources.EventMetadata{}}}, nil
	})
	assert.NoError(t, err)

	storeErr, pubErr := a1.HandleCommand(ctx, commandPub1)
	assert.NoError(t, storeErr)
	assert.NoError(t, pubErr)

	a2, err := MakeAggregate(path2, numEventsInSnapshot, store, publisher, path2topic1, func(context.Context) (AggregateModel, error) {
		return &ResourceStateSnapshotTaken{events.ResourceStateSnapshotTaken{Id: path2.AggregateId, ResourceState: &resources.ResourceState{}, EventMetadata: &resources.EventMetadata{}}}, nil
	})
	assert.NoError(t, err)

	storeErr, pubErr = a2.HandleCommand(ctx, commandPub2)
	assert.NoError(t, storeErr)
	assert.NoError(t, pubErr)

	projection, err := NewProjection(ctx, store, numEventsInSnapshot, subscriber, func(context.Context) (eventstore.Model, error) { return &mockEventHandler{}, nil })
	assert.NoError(t, err)

	err = projection.Project(path1)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(projection.aggregateProjections))

	err = projection.Project(path2)
	assert.NoError(t, err)
	assert.Equal(t, 2, len(projection.aggregateProjections))

	err = projection.SetTopicsToObserve(topics)
	assert.NoError(t, err)

	a3, err := MakeAggregate(path3, numEventsInSnapshot, store, publisher, path2topic2, func(context.Context) (AggregateModel, error) {
		return &ResourceStateSnapshotTaken{events.ResourceStateSnapshotTaken{Id: path3.AggregateId, ResourceState: &resources.ResourceState{}, EventMetadata: &resources.EventMetadata{}}}, nil
	})
	assert.NoError(t, err)

	storeErr, pubErr = a3.HandleCommand(ctx, commandPub3)
	assert.NoError(t, storeErr)
	assert.NoError(t, pubErr)
	time.Sleep(time.Second)
	projection.lock.Lock()
	assert.Equal(t, 3, len(projection.aggregateProjections))
	projection.lock.Unlock()

	storeErr, pubErr = a1.HandleCommand(ctx, commandUnpub1)
	assert.NoError(t, storeErr)
	assert.NoError(t, pubErr)

	err = projection.SetTopicsToObserve(topics[0:1])
	assert.NoError(t, err)

	err = projection.Forget(path3)
	assert.NoError(t, err)

	storeErr, pubErr = a3.HandleCommand(ctx, commandUnpub3)
	assert.NoError(t, storeErr)
	assert.NoError(t, pubErr)
	time.Sleep(time.Second)
	projection.lock.Lock()
	assert.Equal(t, 2, len(projection.aggregateProjections))
	projection.lock.Unlock()

	err = projection.SetTopicsToObserve(nil)
	storeErr, pubErr = a1.HandleCommand(ctx, commandPub1)
	assert.NoError(t, storeErr)
	assert.NoError(t, pubErr)
	projection.lock.Lock()
	assert.Equal(t, 2, len(projection.aggregateProjections))
	projection.lock.Unlock()
}
