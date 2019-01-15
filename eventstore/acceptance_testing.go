// Copyright (c) 2016 - The event.Event Horizon authors.
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

package eventstore

import (
	"context"
	"errors"
	"testing"

	"github.com/globalsign/mgo/bson"
	event "github.com/go-ocf/cqrs/event"
	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
	"github.com/stretchr/testify/assert"
)

type mockEvent struct {
	VersionI     uint64 `bson:"version"`
	EventTypeI   string `bson:"eventtype"`
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
	events []event.Event
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
	eh.events = append(eh.events, e)
	return nil
}

func (eh *mockEventHandler) HandleEventFromStore(ctx context.Context, path protoEvent.Path, eu event.EventUnmarshaler) (bool, error) {
	return true, eh.HandleEvent(ctx, path, eu)
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
func AcceptanceTest(t *testing.T, ctx context.Context, store EventStore) {
	savedEvents := []event.Event{}
	AggregateID1 := "aggregateID1"
	AggregateID2 := "aggregateID2"
	aggregateID1Path := protoEvent.Path{
		AggregateId: AggregateID1,
		Path:        []string{"deviceId"},
	}
	aggregateID2Path := protoEvent.Path{
		AggregateId: AggregateID2,
		Path:        []string{"deviceId"},
	}
	aggregateIDNotExistPath := protoEvent.Path{
		AggregateId: "notExist",
		Path:        []string{"deviceId"},
	}

	eventsToSave := []mockEvent{
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

	out, err := bson.Marshal(&eventsToSave[0])
	assert.NoError(t, err)
	assert.NotEmpty(t, out)

	ctx = context.WithValue(ctx, "testkey", "testval")

	t.Log("save no events")
	conExcep, err := store.Save(ctx, aggregateID1Path, savedEvents)
	assert.Error(t, err)
	assert.False(t, conExcep)

	t.Log("save event, VersionI 0")
	conExcep, err = store.Save(ctx, aggregateID1Path, []event.Event{
		eventsToSave[0],
	})
	assert.NoError(t, err)
	assert.False(t, conExcep)
	savedEvents = append(savedEvents, eventsToSave[0])

	t.Log("save event, VersionI 1")
	conExcep, err = store.Save(ctx, aggregateID1Path, []event.Event{
		eventsToSave[1],
	})
	assert.NoError(t, err)
	assert.False(t, conExcep)
	savedEvents = append(savedEvents, eventsToSave[1])

	t.Log("try to save same event VersionI 1 twice")
	conExcep, err = store.Save(ctx, aggregateID1Path, []event.Event{
		eventsToSave[1],
	})
	assert.True(t, conExcep)
	assert.Error(t, err)

	t.Log("save event, VersionI 2")
	conExcep, err = store.Save(ctx, aggregateID1Path, []event.Event{
		eventsToSave[2],
	})
	assert.NoError(t, err)
	assert.False(t, conExcep)
	savedEvents = append(savedEvents, eventsToSave[2])

	t.Log("save multiple events, VersionI 3, 4 and 5")
	conExcep, err = store.Save(ctx, aggregateID1Path, []event.Event{
		eventsToSave[3], eventsToSave[4], eventsToSave[5],
	})
	assert.NoError(t, err)
	assert.False(t, conExcep)
	savedEvents = append(savedEvents, eventsToSave[3], eventsToSave[4], eventsToSave[5])

	t.Log("save event for another aggregate")
	conExcep, err = store.Save(ctx, aggregateID2Path, []event.Event{
		eventsToSave[6],
	})
	assert.NoError(t, err)
	assert.False(t, conExcep)
	savedEvents = append(savedEvents, eventsToSave[6])

	t.Log("load events for non-existing aggregate")
	var eh1 mockEventHandler
	numEvents, err := store.Load(ctx, aggregateIDNotExistPath, &eh1)
	assert.NoError(t, err)
	assert.Equal(t, 0, numEvents)

	t.Log("load events")
	var eh2 mockEventHandler
	numEvents, err = store.Load(ctx, aggregateID1Path, &eh2)
	assert.Equal(t, len(savedEvents[:6]), numEvents)
	assert.NoError(t, err)
	assert.Equal(t, savedEvents[:6], eh2.events)

	t.Log("load events from version for non-existing aggregate")
	var eh3n mockEventHandler
	numEvents, err = store.LoadFromVersion(ctx, aggregateIDNotExistPath, 0, &eh3n)
	assert.NoError(t, err)
	assert.Equal(t, 0, numEvents)

	t.Log("load events from version")
	var eh3 mockEventHandler
	numEvents, err = store.LoadFromVersion(ctx, aggregateID1Path, savedEvents[2].Version(), &eh3)
	assert.Equal(t, len(savedEvents[2:6]), numEvents)
	assert.NoError(t, err)
	assert.Equal(t, savedEvents[2:6], eh3.events)

	t.Log("load last events with negative limit")
	var eh4error mockEventHandler
	numEvents, err = store.LoadLastEvents(ctx, aggregateIDNotExistPath, -1, &eh4error)
	assert.Error(t, err)
	assert.Equal(t, -1, numEvents)

	t.Log("load last events from non-existing aggregate")
	var eh4n mockEventHandler
	numEvents, err = store.LoadLastEvents(ctx, aggregateIDNotExistPath, 3, &eh4n)
	assert.NoError(t, err)
	assert.Equal(t, 0, numEvents)

	t.Log("load last events")
	var eh4 mockEventHandler
	numEvents, err = store.LoadLastEvents(ctx, aggregateID1Path, len(savedEvents[3:6]), &eh4)
	assert.Equal(t, len(savedEvents[3:6]), numEvents)
	assert.NoError(t, err)
	assert.Equal(t, savedEvents[3:6], eh4.events)

	t.Log("list aggregate paths")
	paths, err := store.ListPaths(ctx, protoEvent.Path{Path: []string{"deviceId"}})
	assert.NoError(t, err)
	assert.Equal(t, []protoEvent.Path{aggregateID1Path, aggregateID2Path}, paths)

	p := MakeProjection(aggregateID1Path, 1, store, func(context.Context) (Model, error) { return &mockEventHandler{}, nil })

	model, numEvents, lastVersion, err := p.Project(ctx)
	assert.NoError(t, err)
	assert.Equal(t, len(savedEvents[:6]), numEvents)
	assert.Equal(t, savedEvents[5].Version(), lastVersion)
	assert.Equal(t, savedEvents[:6], model.(*mockEventHandler).events)

}
