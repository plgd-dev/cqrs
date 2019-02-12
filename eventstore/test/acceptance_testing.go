package test

import (
	"context"
	"errors"
	"testing"

	"github.com/globalsign/mgo/bson"
	event "github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventstore"
	"github.com/stretchr/testify/assert"
)

type mockEvent struct {
	VersionI   uint64 `bson:"version"`
	EventTypeI string `bson:"eventtype"`
	Data       string
}

func (e mockEvent) Version() uint64 {
	return e.VersionI
}

func (e mockEvent) EventType() string {
	return e.EventTypeI
}

type mockEventHandler struct {
	events []event.Event
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
		eh.events = append(eh.events, e)
	}
	return nil
}

func (eh *mockEventHandler) SnapshotEventType() string { return "snapshot" }

// AcceptanceTest is the acceptance test that all implementations of EventStore
// should pass. It should manually be called from a test case in each
// implementation:
//
//   func TestEventStore(t *testing.T) {
//       ctx := context.Background() // Or other when testing namespaces.
//       store := NewEventStore()
//       eventstore.AcceptanceTest(t, ctx, store)
//   }
//
func AcceptanceTest(t *testing.T, ctx context.Context, store eventstore.EventStore) {
	savedEvents := []event.Event{}
	AggregateID1 := "aggregateID1"
	AggregateID2 := "aggregateID2"
	AggregateID3 := "aggregateID1"
	type Path struct {
		GroupId     string
		AggregateId string
	}

	aggregateID1Path := Path{
		AggregateId: AggregateID1,
		GroupId:     "deviceId",
	}
	aggregateID2Path := Path{
		AggregateId: AggregateID2,
		GroupId:     "deviceId",
	}
	aggregateID3Path := Path{
		AggregateId: AggregateID3,
		GroupId:     "deviceId1",
	}

	eventsToSave := []mockEvent{
		mockEvent{
			EventTypeI: "test0",
		},
		mockEvent{
			VersionI:   1,
			EventTypeI: "test1",
		},
		mockEvent{
			VersionI:   2,
			EventTypeI: "test2",
		},
		mockEvent{
			VersionI:   3,
			EventTypeI: "test3",
		},
		mockEvent{
			VersionI:   4,
			EventTypeI: "test4",
		},
		mockEvent{
			VersionI:   5,
			EventTypeI: "test5",
		},
		mockEvent{
			VersionI:   4,
			EventTypeI: "aggr2-test6",
		},
		mockEvent{
			VersionI:   5,
			EventTypeI: "aggr2-test7",
		},
		mockEvent{
			VersionI:   6,
			EventTypeI: "aggr2-test8",
		},
	}

	out, err := bson.Marshal(&eventsToSave[0])
	assert.NoError(t, err)
	assert.NotEmpty(t, out)

	ctx = context.WithValue(ctx, "testkey", "testval")

	t.Log("save no events")
	conExcep, err := store.Save(ctx, aggregateID1Path.GroupId, aggregateID1Path.AggregateId, savedEvents)
	assert.Error(t, err)
	assert.False(t, conExcep)

	t.Log("save event, VersionI 0")
	conExcep, err = store.Save(ctx, aggregateID1Path.GroupId, aggregateID1Path.AggregateId, []event.Event{
		eventsToSave[0],
	})
	assert.NoError(t, err)
	assert.False(t, conExcep)
	savedEvents = append(savedEvents, eventsToSave[0])

	t.Log("save event, VersionI 1")
	conExcep, err = store.Save(ctx, aggregateID1Path.GroupId, aggregateID1Path.AggregateId, []event.Event{
		eventsToSave[1],
	})
	assert.NoError(t, err)
	assert.False(t, conExcep)
	savedEvents = append(savedEvents, eventsToSave[1])

	t.Log("try to save same event VersionI 1 twice")
	conExcep, err = store.Save(ctx, aggregateID1Path.GroupId, aggregateID1Path.AggregateId, []event.Event{
		eventsToSave[1],
	})
	assert.True(t, conExcep)
	assert.Error(t, err)

	t.Log("save event, VersionI 2")
	conExcep, err = store.Save(ctx, aggregateID1Path.GroupId, aggregateID1Path.AggregateId, []event.Event{
		eventsToSave[2],
	})
	assert.NoError(t, err)
	assert.False(t, conExcep)
	savedEvents = append(savedEvents, eventsToSave[2])

	t.Log("save multiple events, VersionI 3, 4 and 5")
	conExcep, err = store.Save(ctx, aggregateID1Path.GroupId, aggregateID1Path.AggregateId, []event.Event{
		eventsToSave[3], eventsToSave[4], eventsToSave[5],
	})
	assert.NoError(t, err)
	assert.False(t, conExcep)
	savedEvents = append(savedEvents, eventsToSave[3], eventsToSave[4], eventsToSave[5])

	t.Log("save event for another aggregate")
	conExcep, err = store.Save(ctx, aggregateID2Path.GroupId, aggregateID2Path.AggregateId, []event.Event{
		eventsToSave[6], eventsToSave[7], eventsToSave[8]})
	assert.NoError(t, err)
	assert.False(t, conExcep)
	savedEvents = append(savedEvents, eventsToSave[6], eventsToSave[7], eventsToSave[8])

	t.Log("load events for non-existing aggregate")
	var eh1 mockEventHandler
	err = store.Load(ctx, []eventstore.Query{{GroupId: "notExist"}}, &eh1)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(eh1.events))

	t.Log("load events")
	var eh2 mockEventHandler
	err = store.Load(ctx, []eventstore.Query{
		{
			GroupId:     aggregateID1Path.GroupId,
			AggregateId: aggregateID1Path.AggregateId,
		},
	}, &eh2)
	assert.NoError(t, err)
	assert.Equal(t, savedEvents[:6], eh2.events)

	t.Log("load events from version")
	var eh3 mockEventHandler
	err = store.Load(ctx, []eventstore.Query{
		{
			GroupId:     aggregateID1Path.GroupId,
			AggregateId: aggregateID1Path.AggregateId,
			FromVersion: savedEvents[2].Version(),
		},
	}, &eh3)
	assert.NoError(t, err)
	assert.Equal(t, savedEvents[2:6], eh3.events)

	t.Log("load multiple aggregatess by all queries")
	var eh4 mockEventHandler
	err = store.Load(ctx, []eventstore.Query{
		{
			GroupId:     aggregateID1Path.GroupId,
			AggregateId: aggregateID1Path.AggregateId,
		},
		{
			GroupId:     aggregateID2Path.GroupId,
			AggregateId: aggregateID2Path.AggregateId,
		},
	}, &eh4)
	assert.NoError(t, err)
	assert.Equal(t, []event.Event{
		eventsToSave[0], eventsToSave[1], eventsToSave[2], eventsToSave[3], eventsToSave[4],
		eventsToSave[6], eventsToSave[5], eventsToSave[7], eventsToSave[8],
	}, eh4.events)

	t.Log("load multiple aggregates by groupId")
	var eh5 mockEventHandler
	err = store.Load(ctx, []eventstore.Query{
		{
			GroupId: aggregateID1Path.GroupId,
		},
	}, &eh5)
	assert.NoError(t, err)
	assert.Equal(t, []event.Event{
		eventsToSave[0], eventsToSave[1], eventsToSave[2], eventsToSave[3], eventsToSave[4],
		eventsToSave[6], eventsToSave[5], eventsToSave[7], eventsToSave[8],
	}, eh5.events)

	t.Log("load multiple aggregates by all")
	var eh6 mockEventHandler
	conExcep, err = store.Save(ctx, aggregateID3Path.GroupId, aggregateID3Path.AggregateId, []event.Event{eventsToSave[0]})
	assert.NoError(t, err)
	assert.False(t, conExcep)
	err = store.Load(ctx, []eventstore.Query{}, &eh6)
	assert.NoError(t, err)
	assert.Equal(t, []event.Event{
		eventsToSave[0], eventsToSave[0], eventsToSave[1], eventsToSave[2], eventsToSave[3], eventsToSave[4],
		eventsToSave[6], eventsToSave[5], eventsToSave[7], eventsToSave[8],
	}, eh6.events)

	t.Log("test projection all")
	model := mockEventHandler{}
	p := eventstore.NewProjection(10, store, func(context.Context) (eventstore.Model, error) { return &model, nil })

	err = p.Project(ctx, []eventstore.Query{})
	assert.NoError(t, err)
	assert.Equal(t, []event.Event{
		eventsToSave[0], eventsToSave[1], eventsToSave[2], eventsToSave[3], eventsToSave[4], eventsToSave[5],
		eventsToSave[0],
	}, model.events)

	t.Log("test projection group")
	model1 := mockEventHandler{}
	p = eventstore.NewProjection(10, store, func(context.Context) (eventstore.Model, error) { return &model1, nil })

	err = p.Project(ctx, []eventstore.Query{eventstore.Query{GroupId: aggregateID1Path.GroupId}})
	assert.NoError(t, err)
	assert.Equal(t, []event.Event{
		eventsToSave[0], eventsToSave[1], eventsToSave[2], eventsToSave[3], eventsToSave[4], eventsToSave[5],
	}, model1.events)

	t.Log("test projection aggregate")
	model2 := mockEventHandler{}
	p = eventstore.NewProjection(10, store, func(context.Context) (eventstore.Model, error) { return &model2, nil })

	err = p.Project(ctx, []eventstore.Query{
		eventstore.Query{
			GroupId:     aggregateID2Path.GroupId,
			AggregateId: aggregateID2Path.AggregateId,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, mockEventHandler{events: []event.Event(nil)}, model2)

}
