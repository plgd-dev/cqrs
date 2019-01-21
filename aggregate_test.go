package cqrs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	resources "github.com/go-ocf/resource-aggregate/protobuf"
	"github.com/go-ocf/resource-aggregate/protobuf/commands"

	"github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventstore"
	"github.com/go-ocf/cqrs/eventstore/mongodb"
	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
	"github.com/go-ocf/kit/http"
	"github.com/go-ocf/resource-aggregate/protobuf/events"
	"github.com/stretchr/testify/assert"
)

type ResourcePublished struct {
	events.ResourcePublished
}

func (e ResourcePublished) Version() uint64 {
	return e.EventMetadata.Version
}

func (e ResourcePublished) Marshal() ([]byte, error) {
	return e.ResourcePublished.Marshal()
}

func (e *ResourcePublished) Unmarshal(b []byte) error {
	return e.ResourcePublished.Unmarshal(b)
}

func (e ResourcePublished) EventType() string {
	return http.ProtobufContentType(&events.ResourcePublished{})
}

func (e ResourcePublished) AggregateId() string {
	return e.Id
}

type ResourceUnpublished struct {
	events.ResourceUnpublished
}

func (e ResourceUnpublished) Version() uint64 {
	return e.EventMetadata.Version
}

func (e ResourceUnpublished) Marshal() ([]byte, error) {
	return e.ResourceUnpublished.Marshal()
}

func (e *ResourceUnpublished) Unmarshal(b []byte) error {
	return e.ResourceUnpublished.Unmarshal(b)
}

func (e ResourceUnpublished) EventType() string {
	return http.ProtobufContentType(&events.ResourceUnpublished{})
}

func (e ResourceUnpublished) AggregateId() string {
	return e.Id
}

type ResourceStateSnapshotTaken struct {
	events.ResourceStateSnapshotTaken
}

func (rs *ResourceStateSnapshotTaken) AggregateId() string {
	return rs.Id
}

func (rs *ResourceStateSnapshotTaken) Version() uint64 {
	return rs.EventMetadata.Version
}

func (rs *ResourceStateSnapshotTaken) Marshal() ([]byte, error) {
	return rs.ResourceStateSnapshotTaken.Marshal()
}

func (rs *ResourceStateSnapshotTaken) Unmarshal(b []byte) error {
	return rs.ResourceStateSnapshotTaken.Unmarshal(b)
}

func (rs *ResourceStateSnapshotTaken) EventType() string {
	return http.ProtobufContentType(&events.ResourceStateSnapshotTaken{})
}

func (rs *ResourceStateSnapshotTaken) HandleEventResourcePublished(ctx context.Context, pub ResourcePublished) error {
	if rs.ResourceState.IsPublished {
		return fmt.Errorf("already published")
	}
	rs.ResourceState.IsPublished = true
	return nil
}

func (rs *ResourceStateSnapshotTaken) HandleEventResourceUnpublished(ctx context.Context, pub ResourceUnpublished) error {
	if !rs.ResourceState.IsPublished {
		return fmt.Errorf("already published")
	}
	rs.ResourceState.IsPublished = false
	return nil
}

func (rs *ResourceStateSnapshotTaken) HandleEvent(ctx context.Context, path protoEvent.Path, iter event.Iter) error {
	var eu event.EventUnmarshaler
	for iter.Next(&eu) {
		if eu.EventType == "" {
			return errors.New("cannot determine type of event")
		}
		switch eu.EventType {
		case http.ProtobufContentType(&events.ResourceStateSnapshotTaken{}):
			var s ResourceStateSnapshotTaken
			if err := eu.Unmarshal(&s); err != nil {
				return err
			}
			*rs = s
			return nil
		case http.ProtobufContentType(&events.ResourcePublished{}):
			var s ResourcePublished
			if err := eu.Unmarshal(&s); err != nil {
				return err
			}
			return rs.HandleEventResourcePublished(ctx, s)
		case http.ProtobufContentType(&events.ResourceUnpublished{}):
			var s ResourceUnpublished
			if err := eu.Unmarshal(&s); err != nil {
				return err
			}
			return rs.HandleEventResourceUnpublished(ctx, s)
		}
	}
	return nil
}

const CorrelationID = "CorrelationID"

func TimeNowMs() uint64 {
	now := time.Now()
	unix := now.UnixNano()
	return uint64(unix / int64(time.Millisecond))
}

//CreateEventMeta for creating EventMetadata from ResourcefModel
func CreateEventMeta(newVersion uint64) resources.EventMetadata {
	return resources.EventMetadata{
		Version:     newVersion,
		TimestampMs: TimeNowMs(),
	}
}

func CreateAuditContext(a *commands.AuthorizationContext, correlationId string) resources.AuditContext {
	return resources.AuditContext{
		UserId:        a.UserId,
		DeviceId:      a.DeviceId,
		CorrelationId: correlationId,
	}
}

func (rs *ResourceStateSnapshotTaken) HandleCommand(ctx context.Context, cmd Command, newVersion uint64) ([]event.Event, error) {
	switch req := cmd.(type) {
	case commands.PublishResourceRequest:
		correlationId, _ := ctx.Value(CorrelationID).(string)
		ac := CreateAuditContext(req.AuthorizationContext, correlationId)

		em := CreateEventMeta(newVersion)
		rp := ResourcePublished{events.ResourcePublished{
			Id:            req.ResourceId,
			Resource:      req.Resource,
			TimeToLive:    req.TimeToLive,
			AuditContext:  &ac,
			EventMetadata: &em,
		},
		}
		err := rs.HandleEventResourcePublished(ctx, rp)
		if err != nil {
			return nil, fmt.Errorf("cannot handle resource publish: %v", err)
		}
		return []event.Event{rp}, nil
	case commands.UnpublishResourceRequest:
		correlationId, _ := ctx.Value(CorrelationID).(string)
		ac := CreateAuditContext(req.AuthorizationContext, correlationId)
		em := CreateEventMeta(newVersion)
		ru := ResourceUnpublished{events.ResourceUnpublished{
			Id:            req.ResourceId,
			AuditContext:  &ac,
			EventMetadata: &em,
		}}
		err := rs.HandleEventResourceUnpublished(ctx, ru)
		if err != nil {
			return nil, fmt.Errorf("cannot handle resource publish: %v", err)
		}
		return []event.Event{ru}, nil
	}

	return nil, fmt.Errorf("unknown command")
}

func (rs *ResourceStateSnapshotTaken) SnapshotEventType() string { return rs.EventType() }

func (rs *ResourceStateSnapshotTaken) TakeSnapshot(version uint64) (event.Event, error) {
	rs.EventMetadata.Version = version
	return rs, nil
}

type mockEventHandler struct {
	events []event.EventUnmarshaler
}

func (eh *mockEventHandler) BeforeLoadingEventsFromEventstore(ctx context.Context, path protoEvent.Path) {

}

func (eh *mockEventHandler) AfterLoadingEventsFromEventstore(ctx context.Context, path protoEvent.Path) {

}

func (eh *mockEventHandler) HandleEvent(ctx context.Context, path protoEvent.Path, iter event.Iter) error {
	var eu event.EventUnmarshaler
	for iter.Next(&eu) {
		if eu.EventType == "" {
			return errors.New("cannot determine type of event")
		}
		eh.events = append(eh.events, eu)
	}
	return nil
}

func (eh *mockEventHandler) SnapshotEventType() string {
	var rs ResourceStateSnapshotTaken
	return rs.SnapshotEventType()
}

type ProtobufMarshaler interface {
	Marshal() ([]byte, error)
}

type ProtobufUnmarshaler interface {
	Unmarshal([]byte) error
}

func TestAggregate(t *testing.T) {
	// Local Mongo testing with Docker
	url := os.Getenv("MONGO_HOST")

	if url == "" {
		// Default to localhost
		url = "localhost:27017"
	}

	store, err := mongodb.NewEventStore(url, "test_aggregate", "events", func(v interface{}) ([]byte, error) {
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

	path := protoEvent.Path{
		Path:        []string{"1", "2", "3", "4"},
		AggregateId: "ID2",
	}

	commandPub := commands.PublishResourceRequest{
		ResourceId: path.AggregateId,
		Resource: &resources.Resource{
			Id: path.AggregateId,
		},
		AuthorizationContext: &commands.AuthorizationContext{},
	}

	commandUnpub := commands.UnpublishResourceRequest{
		ResourceId:           path.AggregateId,
		AuthorizationContext: &commands.AuthorizationContext{},
	}

	a, err := NewAggregate(path, store, 1, func(context.Context) (AggregateModel, error) {
		return &ResourceStateSnapshotTaken{events.ResourceStateSnapshotTaken{Id: path.AggregateId, ResourceState: &resources.ResourceState{}, EventMetadata: &resources.EventMetadata{}}}, nil
	})
	assert.NoError(t, err)

	events, err := a.HandleCommand(ctx, commandPub)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	events, err = a.HandleCommand(ctx, commandPub)
	assert.Error(t, err)
	assert.Nil(t, events)

	events, err = a.HandleCommand(ctx, commandUnpub)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	events, err = a.HandleCommand(ctx, commandUnpub)
	assert.Error(t, err)
	assert.Nil(t, events)

	events, err = a.HandleCommand(ctx, commandPub)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	events, err = a.HandleCommand(ctx, commandUnpub)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	p := eventstore.MakeProjection(path, 1, store, func(context.Context) (eventstore.Model, error) { return &mockEventHandler{}, nil })

	_, numEvents, lastVersion, err := p.Project(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 1, numEvents)
	assert.Equal(t, uint64(6), lastVersion)

	//assert.Equal(t, nil, model.(*mockEventHandler).events)

}
