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
	"github.com/go-ocf/cqrs/test/pb"
	"github.com/go-ocf/kit/net/http"
	"github.com/stretchr/testify/require"
)

type ResourcePublished struct {
	pb.ResourcePublished
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
	return http.ProtobufContentType(&pb.ResourcePublished{})
}

func (e ResourcePublished) AggregateId() string {
	return e.Id
}

type ResourceUnpublished struct {
	pb.ResourceUnpublished
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
	return http.ProtobufContentType(&pb.ResourceUnpublished{})
}

func (e ResourceUnpublished) AggregateId() string {
	return e.Id
}

type ResourceStateSnapshotTaken struct {
	pb.ResourceStateSnapshotTaken
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
	return http.ProtobufContentType(&pb.ResourceStateSnapshotTaken{})
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
		case http.ProtobufContentType(&pb.ResourceStateSnapshotTaken{}):
			var s pb.ResourceStateSnapshotTaken
			if err := eu.Unmarshal(&s); err != nil {
				return err
			}
			rs.ResourceStateSnapshotTaken = s
		case http.ProtobufContentType(&pb.ResourcePublished{}):
			var s ResourcePublished
			if err := eu.Unmarshal(&s); err != nil {
				return err
			}
			if err := rs.HandleEventResourcePublished(ctx, s); err != nil {
				return err
			}
		case http.ProtobufContentType(&pb.ResourceUnpublished{}):
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
func CreateEventMeta(newVersion uint64) pb.EventMetadata {
	return pb.EventMetadata{
		Version:     newVersion,
		TimestampMs: TimeNowMs(),
	}
}

func CreateAuditContext(a *pb.AuthorizationContext, correlationId string) pb.AuditContext {
	return pb.AuditContext{
		UserId:        a.UserId,
		DeviceId:      a.DeviceId,
		CorrelationId: correlationId,
	}
}

func (rs *ResourceStateSnapshotTaken) HandleCommand(ctx context.Context, cmd Command, newVersion uint64) ([]event.Event, error) {
	switch req := cmd.(type) {
	case pb.PublishResourceRequest:
		correlationId, _ := ctx.Value(CorrelationID).(string)
		ac := CreateAuditContext(req.AuthorizationContext, correlationId)

		em := CreateEventMeta(newVersion)
		rp := ResourcePublished{pb.ResourcePublished{
			Id:            req.ResourceId,
			Resource:      req.Resource,
			TimeToLive:    req.TimeToLive,
			AuditContext:  &ac,
			EventMetadata: &em,
		},
		}
		err := rs.HandleEventResourcePublished(ctx, rp)
		if err != nil {
			return nil, fmt.Errorf("cannot handle resource publish: %w", err)
		}
		return []event.Event{rp}, nil
	case pb.UnpublishResourceRequest:
		correlationId, _ := ctx.Value(CorrelationID).(string)
		ac := CreateAuditContext(req.AuthorizationContext, correlationId)
		em := CreateEventMeta(newVersion)
		ru := ResourceUnpublished{pb.ResourceUnpublished{
			Id:            req.ResourceId,
			AuditContext:  &ac,
			EventMetadata: &em,
		}}
		err := rs.HandleEventResourceUnpublished(ctx, ru)
		if err != nil {
			return nil, fmt.Errorf("cannot handle resource unpublish: %w", err)
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
	pb []event.EventUnmarshaler
}

func (eh *mockEventHandler) Handle(ctx context.Context, iter event.Iter) error {
	var eu event.EventUnmarshaler
	for iter.Next(ctx, &eu) {
		if eu.EventType == "" {
			return errors.New("cannot determine type of event")
		}
		eh.pb = append(eh.pb, eu)
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
	host := os.Getenv("MONGO_HOST")

	if host == "" {
		// Default to localhost
		host = "localhost:27017"
	}

	store, err := mongodb.NewEventStore(context.Background(), host, "test_aggregate", "pb", 2, nil, func(v interface{}) ([]byte, error) {
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

	return store
}

func TestAggregate(t *testing.T) {
	store := testNewEventstore(t)
	ctx := context.Background()
	defer store.Close(ctx)
	defer func() {
		err := store.Clear(ctx)
		require.NoError(t, err)
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

	commandPub := pb.PublishResourceRequest{
		ResourceId: path.AggregateId,
		Resource: &pb.Resource{
			Id: path.AggregateId,
		},
		AuthorizationContext: &pb.AuthorizationContext{},
	}

	commandUnpub := pb.UnpublishResourceRequest{
		ResourceId:           path.AggregateId,
		AuthorizationContext: &pb.AuthorizationContext{},
	}

	commandPub1 := pb.PublishResourceRequest{
		ResourceId: path1.AggregateId,
		Resource: &pb.Resource{
			Id: path1.AggregateId,
		},
		AuthorizationContext: &pb.AuthorizationContext{},
	}

	commandUnpub1 := pb.UnpublishResourceRequest{
		ResourceId:           path1.AggregateId,
		AuthorizationContext: &pb.AuthorizationContext{},
	}

	newAggragate := func() *Aggregate {
		a, err := NewAggregate(path.AggregateId, NewDefaultRetryFunc(1), 2, store, func(context.Context) (AggregateModel, error) {
			return &ResourceStateSnapshotTaken{pb.ResourceStateSnapshotTaken{Id: path.AggregateId, Resource: &pb.Resource{}, EventMetadata: &pb.EventMetadata{}}}, nil
		}, nil)
		require.NoError(t, err)
		return a
	}

	a := newAggragate()
	pb, err := a.HandleCommand(ctx, commandPub)
	require.NoError(t, err)
	require.NotNil(t, pb)

	b := newAggragate()
	pb, err = b.HandleCommand(ctx, commandPub)
	require.Error(t, err)
	require.Nil(t, pb)

	c := newAggragate()
	pb, err = c.HandleCommand(ctx, commandUnpub)
	require.NoError(t, err)
	require.NotNil(t, pb)

	d := newAggragate()
	pb, err = d.HandleCommand(ctx, commandUnpub)
	require.Error(t, err)
	require.Nil(t, pb)

	e := newAggragate()
	pb, err = e.HandleCommand(ctx, commandPub1)
	require.NoError(t, err)
	require.NotNil(t, pb)

	f := newAggragate()
	pb, err = f.HandleCommand(ctx, commandUnpub1)
	require.NoError(t, err)
	require.NotNil(t, pb)

	g := newAggragate()
	pb, err = g.HandleCommand(ctx, commandPub)
	require.NoError(t, err)
	require.NotNil(t, pb)

	h := newAggragate()
	pb, err = h.HandleCommand(ctx, commandUnpub)
	require.NoError(t, err)
	require.NotNil(t, pb)

	handler := &mockEventHandler{}
	p := eventstore.NewProjection(store, func(context.Context) (eventstore.Model, error) { return handler, nil }, nil)

	err = p.Project(ctx, []eventstore.SnapshotQuery{
		eventstore.SnapshotQuery{
			GroupId:           path.GroupId,
			AggregateId:       path.AggregateId,
			SnapshotEventType: handler.SnapshotEventType(),
		},
	})
	require.NoError(t, err)

	//require.Equal(t, nil, model.(*mockEventHandler).pb)

	concurrencyExcepTestA := newAggragate()
	model, err := concurrencyExcepTestA.factoryModel(ctx)
	require.NoError(t, err)

	amodel, err := newAggrModel(ctx, a.aggregateId, a.store, a.LogDebugfFunc, model)
	require.NoError(t, err)

	pb, concurrencyException, err := a.handleCommandWithAggrModel(ctx, commandPub, amodel)
	require.NoError(t, err)
	require.False(t, concurrencyException)
	require.NotNil(t, pb)

	pb, concurrencyException, err = a.handleCommandWithAggrModel(ctx, commandUnpub, amodel)
	require.NoError(t, nil)
	require.True(t, concurrencyException)
	require.Nil(t, pb)
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
