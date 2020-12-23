package cqrs

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gofrs/uuid"
	"github.com/nats-io/nats.go"
	"github.com/panjf2000/ants/v2"
	"github.com/plgd-dev/cqrs/test/pb"
	"github.com/stretchr/testify/assert"

	eventbus "github.com/plgd-dev/cqrs/eventbus/nats"
	"github.com/plgd-dev/cqrs/eventstore"
	"github.com/plgd-dev/cqrs/eventstore/mongodb"
	"github.com/stretchr/testify/require"
)

func TestProjection(t *testing.T) {
	numEventsInSnapshot := 1
	waitForSubscription := time.Second * 1

	topics := []string{"test_projection_topic0_" + uuid.Must(uuid.NewV4()).String(), "test_projection_topic1_" + uuid.Must(uuid.NewV4()).String()}

	publisher, err := eventbus.NewPublisher(nats.DefaultURL, json.Marshal)
	assert.NoError(t, err)
	assert.NotNil(t, publisher)
	defer publisher.Close()

	subscriber, err := eventbus.NewSubscriber(nats.DefaultURL,
		json.Unmarshal,
		func(f func()) error { go f(); return nil },
		func(err error) {
			assert.NoError(t, err)
		},
	)
	assert.NotNil(t, subscriber)
	assert.NoError(t, err)
	defer subscriber.Close()

	// Local Mongo testing with Docker
	host := os.Getenv("MONGO_HOST")

	if host == "" {
		// Default to localhost
		host = "localhost:27017"
	}

	pool, err := ants.NewPool(16)
	require.NoError(t, err)
	defer pool.Release()

	ctx := context.Background()

	store, err := mongodb.NewEventStore(
		ctx,
		host,
		"test_projection",
		"pb",
		128,
		func(f func()) error { go f(); return nil },
		func(v interface{}) ([]byte, error) {
			if p, ok := v.(ProtobufMarshaler); ok {
				return p.Marshal()
			}
			return nil, fmt.Errorf("marshal is not supported by %T", v)
		}, func(b []byte, v interface{}) error {
			if p, ok := v.(ProtobufUnmarshaler); ok {
				return p.Unmarshal(b)
			}
			return fmt.Errorf("marshal is not supported by %T", v)
		}, nil)
	/*bson.Marshal, bson.Unmarshal*/
	require.NoError(t, err)
	require.NotNil(t, store)

	defer store.Close(ctx)
	defer func() {
		err = store.Clear(ctx)
		require.NoError(t, err)
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

	commandPub1 := pb.PublishResourceRequest{
		ResourceId: path1.AggregateId,
		Resource: &pb.Resource{
			Id:       path1.AggregateId,
			DeviceId: path1.GroupId,
		},
		AuthorizationContext: &pb.AuthorizationContext{},
	}

	commandPub2 := pb.PublishResourceRequest{
		ResourceId: path2.AggregateId,
		Resource: &pb.Resource{
			Id:       path2.AggregateId,
			DeviceId: path2.GroupId,
		},
		AuthorizationContext: &pb.AuthorizationContext{},
	}

	commandPub3 := pb.PublishResourceRequest{
		ResourceId: path3.AggregateId,
		Resource: &pb.Resource{
			Id:       path3.AggregateId,
			DeviceId: path3.GroupId,
		},
		AuthorizationContext: &pb.AuthorizationContext{},
	}

	commandUnpub1 := pb.UnpublishResourceRequest{
		ResourceId:           path1.AggregateId,
		AuthorizationContext: &pb.AuthorizationContext{},
	}

	commandUnpub3 := pb.UnpublishResourceRequest{
		ResourceId:           path3.AggregateId,
		AuthorizationContext: &pb.AuthorizationContext{},
	}

	/*
		path2topics := func(path Path, event event.Event) []string {
			return topics
		}
	*/

	a1, err := NewAggregate(path1.GroupId, path1.AggregateId, NewDefaultRetryFunc(1), numEventsInSnapshot, store, func(context.Context) (AggregateModel, error) {
		return &ResourceStateSnapshotTaken{pb.ResourceStateSnapshotTaken{Id: path1.AggregateId, Resource: &pb.Resource{
			DeviceId: path1.GroupId,
		}, EventMetadata: &pb.EventMetadata{}}}, nil
	}, nil)
	require.NoError(t, err)

	evs, err := a1.HandleCommand(ctx, commandPub1)
	require.NoError(t, err)
	require.NotNil(t, evs)

	snapshotEventType := func() string {
		s := &ResourceStateSnapshotTaken{}
		return s.SnapshotEventType()
	}

	a2, err := NewAggregate(path2.GroupId, path2.AggregateId, NewDefaultRetryFunc(1), numEventsInSnapshot, store, func(context.Context) (AggregateModel, error) {
		return &ResourceStateSnapshotTaken{pb.ResourceStateSnapshotTaken{Id: path2.AggregateId, Resource: &pb.Resource{
			DeviceId: path2.GroupId,
		}, EventMetadata: &pb.EventMetadata{}}}, nil
	}, nil)
	require.NoError(t, err)

	evs, err = a2.HandleCommand(ctx, commandPub2)
	require.NoError(t, err)
	require.NotNil(t, evs)

	projection, err := NewProjection(ctx, store, "testProjection", subscriber, func(context.Context) (eventstore.Model, error) { return &mockEventHandler{}, nil }, nil)
	require.NoError(t, err)

	err = projection.Project(ctx, []eventstore.SnapshotQuery{
		eventstore.SnapshotQuery{
			GroupID:           path1.GroupId,
			AggregateID:       path1.AggregateId,
			SnapshotEventType: snapshotEventType(),
		},
	})
	require.NoError(t, err)
	require.Equal(t, 1, len(projection.Models(nil)))

	err = projection.Project(ctx, []eventstore.SnapshotQuery{
		eventstore.SnapshotQuery{
			GroupID:           path2.GroupId,
			AggregateID:       path2.AggregateId,
			SnapshotEventType: snapshotEventType(),
		},
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(projection.Models(nil)))

	err = projection.SubscribeTo(topics)
	require.NoError(t, err)

	time.Sleep(waitForSubscription)

	a3, err := NewAggregate(path3.GroupId, path3.AggregateId, NewDefaultRetryFunc(1), numEventsInSnapshot, store, func(context.Context) (AggregateModel, error) {
		return &ResourceStateSnapshotTaken{pb.ResourceStateSnapshotTaken{Id: path3.AggregateId, Resource: &pb.Resource{
			DeviceId: path3.GroupId,
		}, EventMetadata: &pb.EventMetadata{}}}, nil
	}, nil)
	require.NoError(t, err)

	evs, err = a3.HandleCommand(ctx, commandPub3)
	require.NoError(t, err)
	require.NotNil(t, evs)
	for _, e := range evs {
		err = publisher.Publish(ctx, topics, path3.GroupId, path3.AggregateId, e)
		require.NoError(t, err)
	}
	time.Sleep(time.Second)

	require.Equal(t, 3, len(projection.Models(nil)))

	evs, err = a1.HandleCommand(ctx, commandUnpub1)
	require.NoError(t, err)
	require.NotNil(t, evs)
	for _, e := range evs {
		err = publisher.Publish(ctx, topics, path3.GroupId, path3.AggregateId, e)
		require.NoError(t, err)
	}

	err = projection.SubscribeTo(topics[0:1])
	require.NoError(t, err)

	time.Sleep(waitForSubscription)

	err = projection.Forget([]eventstore.SnapshotQuery{
		eventstore.SnapshotQuery{
			GroupID:           path3.GroupId,
			AggregateID:       path3.AggregateId,
			SnapshotEventType: snapshotEventType(),
		},
	})
	require.NoError(t, err)

	evs, err = a3.HandleCommand(ctx, commandUnpub3)
	require.NoError(t, err)
	require.NotNil(t, evs)
	for _, e := range evs {
		err = publisher.Publish(ctx, topics[1:], path3.GroupId, path3.AggregateId, e)
		require.NoError(t, err)
	}

	time.Sleep(time.Second)
	projection.lock.Lock()
	require.Equal(t, 2, len(projection.Models(nil)))
	projection.lock.Unlock()

	err = projection.SubscribeTo(nil)
	require.NoError(t, err)

	time.Sleep(waitForSubscription)

	evs, err = a1.HandleCommand(ctx, commandPub1)
	require.NoError(t, err)
	require.NotNil(t, evs)
	for _, e := range evs {
		err = publisher.Publish(ctx, topics, path1.GroupId, path1.AggregateId, e)
		require.NoError(t, err)
	}

	projection.lock.Lock()
	require.Equal(t, 2, len(projection.Models(nil)))
	projection.lock.Unlock()
}
