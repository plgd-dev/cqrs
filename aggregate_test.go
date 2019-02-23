package cqrs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventstore"
	"github.com/go-ocf/cqrs/eventstore/mongodb"
	kitCqrsProto "github.com/go-ocf/kit/cqrs/protobuf"
	"github.com/go-ocf/kit/http"
	resources "github.com/go-ocf/resource-aggregate/protobuf"
	"github.com/go-ocf/resource-aggregate/protobuf/commands"
	"github.com/go-ocf/resource-aggregate/protobuf/events"
	"github.com/panjf2000/ants"
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
	if rs.IsPublished {
		return fmt.Errorf("already published")
	}
	rs.IsPublished = true
	return nil
}

func (rs *ResourceStateSnapshotTaken) HandleEventResourceUnpublished(ctx context.Context, pub ResourceUnpublished) error {
	if !rs.IsPublished {
		return fmt.Errorf("already unpublished")
	}
	rs.IsPublished = false
	return nil
}

func (rs *ResourceStateSnapshotTaken) Handle(ctx context.Context, iter event.Iter) error {
	var eu event.EventUnmarshaler
	for iter.Next(ctx, &eu) {
		if eu.EventType == "" {
			return errors.New("cannot determine type of event")
		}
		switch eu.EventType {
		case http.ProtobufContentType(&events.ResourceStateSnapshotTaken{}):
			var s events.ResourceStateSnapshotTaken
			if err := eu.Unmarshal(&s); err != nil {
				return err
			}
			rs.ResourceStateSnapshotTaken = s
		case http.ProtobufContentType(&events.ResourcePublished{}):
			var s ResourcePublished
			if err := eu.Unmarshal(&s); err != nil {
				return err
			}
			if err := rs.HandleEventResourcePublished(ctx, s); err != nil {
				return err
			}
		case http.ProtobufContentType(&events.ResourceUnpublished{}):
			var s ResourceUnpublished
			if err := eu.Unmarshal(&s); err != nil {
				return err
			}
			if err := rs.HandleEventResourceUnpublished(ctx, s); err != nil {
				return err
			}
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
func CreateEventMeta(newVersion uint64) kitCqrsProto.EventMetadata {
	return kitCqrsProto.EventMetadata{
		Version:     newVersion,
		TimestampMs: TimeNowMs(),
	}
}

func CreateAuditContext(a *kitCqrsProto.AuthorizationContext, correlationId string) kitCqrsProto.AuditContext {
	return kitCqrsProto.AuditContext{
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
			return nil, fmt.Errorf("cannot handle resource unpublish: %v", err)
		}
		return []event.Event{ru}, nil
	}

	return nil, fmt.Errorf("unknown command")
}

func (rs *ResourceStateSnapshotTaken) SnapshotEventType() string { return rs.EventType() }

func (rs *ResourceStateSnapshotTaken) GroupId() string {
	return rs.Resource.DeviceId
}

func (rs *ResourceStateSnapshotTaken) TakeSnapshot(version uint64) (event.Event, bool) {
	rs.EventMetadata.Version = version
	return rs, true
}

type mockEventHandler struct {
	events []event.EventUnmarshaler
}

func (eh *mockEventHandler) Handle(ctx context.Context, iter event.Iter) error {
	var eu event.EventUnmarshaler
	for iter.Next(ctx, &eu) {
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

func testNewEventstore(t *testing.T) *mongodb.EventStore {
	// Local Mongo testing with Docker
	url := os.Getenv("MONGO_HOST")

	if url == "" {
		// Default to localhost
		url = "localhost:27017"
	}

	var pool *ants.Pool

	store, err := mongodb.NewEventStore(url, "test_aggregate", "events", 128, pool, func(v interface{}) ([]byte, error) {
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
	assert.NoError(t, err)
	assert.NotNil(t, store)

	return store
}

func TestAggregate(t *testing.T) {
	store := testNewEventstore(t)
	ctx := context.Background()
	defer store.Close()
	defer func() {
		err := store.Clear(ctx)
		assert.NoError(t, err)
	}()

	type Path struct {
		GroupId     string
		AggregateId string
	}

	path := Path{
		GroupId:     "1",
		AggregateId: "ID0",
	}

	path1 := Path{
		GroupId:     "1",
		AggregateId: "ID1",
	}

	commandPub := commands.PublishResourceRequest{
		ResourceId: path.AggregateId,
		Resource: &resources.Resource{
			Id: path.AggregateId,
		},
		AuthorizationContext: &kitCqrsProto.AuthorizationContext{},
	}

	commandUnpub := commands.UnpublishResourceRequest{
		ResourceId:           path.AggregateId,
		AuthorizationContext: &kitCqrsProto.AuthorizationContext{},
	}

	commandPub1 := commands.PublishResourceRequest{
		ResourceId: path1.AggregateId,
		Resource: &resources.Resource{
			Id: path1.AggregateId,
		},
		AuthorizationContext: &kitCqrsProto.AuthorizationContext{},
	}

	commandUnpub1 := commands.UnpublishResourceRequest{
		ResourceId:           path1.AggregateId,
		AuthorizationContext: &kitCqrsProto.AuthorizationContext{},
	}

	newAggragate := func() *Aggregate {
		a, err := NewAggregate(path.AggregateId, NewDefaultRetryFunc(1), 128, store, func(context.Context) (AggregateModel, error) {
			return &ResourceStateSnapshotTaken{events.ResourceStateSnapshotTaken{Id: path.AggregateId, Resource: &resources.Resource{}, EventMetadata: &kitCqrsProto.EventMetadata{}}}, nil
		}, nil)
		assert.NoError(t, err)
		return a
	}

	a := newAggragate()
	events, err := a.HandleCommand(ctx, commandPub)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	b := newAggragate()
	events, err = b.HandleCommand(ctx, commandPub)
	assert.Error(t, err)
	assert.Nil(t, events)

	c := newAggragate()
	events, err = c.HandleCommand(ctx, commandUnpub)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	d := newAggragate()
	events, err = d.HandleCommand(ctx, commandUnpub)
	assert.Error(t, err)
	assert.Nil(t, events)

	e := newAggragate()
	events, err = e.HandleCommand(ctx, commandPub1)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	f := newAggragate()
	events, err = f.HandleCommand(ctx, commandUnpub1)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	g := newAggragate()
	events, err = g.HandleCommand(ctx, commandPub)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	h := newAggragate()
	events, err = h.HandleCommand(ctx, commandUnpub)
	assert.NoError(t, err)
	assert.NotNil(t, events)

	handler := &mockEventHandler{}
	p := eventstore.NewProjection(store, func(context.Context) (eventstore.Model, error) { return handler, nil }, nil)

	err = p.Project(ctx, []eventstore.SnapshotQuery{
		eventstore.SnapshotQuery{
			GroupId:           path.GroupId,
			AggregateId:       path.AggregateId,
			SnapshotEventType: handler.SnapshotEventType(),
		},
	})
	assert.NoError(t, err)

	//assert.Equal(t, nil, model.(*mockEventHandler).events)

	concurrencyExcepTestA := newAggragate()
	model, err := concurrencyExcepTestA.factoryModel(ctx)
	assert.NoError(t, err)

	amodel, err := newAggrModel(ctx, a.aggregateId, a.store, a.LogDebugfFunc, model)
	assert.NoError(t, err)

	events, concurrencyException, err := a.handleCommandWithAggrModel(ctx, commandPub, amodel)
	assert.NoError(t, err)
	assert.False(t, concurrencyException)
	assert.NotNil(t, events)

	events, concurrencyException, err = a.handleCommandWithAggrModel(ctx, commandUnpub, amodel)
	assert.NoError(t, nil)
	assert.True(t, concurrencyException)
	assert.Nil(t, events)
}

func canceledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func Test_handleRetry(t *testing.T) {
	type args struct {
		ctx       context.Context
		retryFunc RetryFunc
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "ok",
			args: args{
				ctx:       context.Background(),
				retryFunc: func() (time.Time, error) { return time.Now(), nil },
			},
			wantErr: false,
		},
		{
			name: "err",
			args: args{
				ctx:       context.Background(),
				retryFunc: func() (time.Time, error) { return time.Now().Add(time.Second), errors.New("error") },
			},
			wantErr: true,
		},
		{
			name: "canceled",
			args: args{
				ctx:       canceledContext(),
				retryFunc: func() (time.Time, error) { return time.Now().Add(time.Second), nil },
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := handleRetry(tt.args.ctx, tt.args.retryFunc); (err != nil) != tt.wantErr {
				t.Errorf("handleRetry() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
