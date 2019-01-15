// Copyright (c) 2015 - The Event Horizon authors
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

package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/globalsign/mgo/txn"

	"github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventstore"

	protoEvent "github.com/go-ocf/cqrs/protobuf/event"
)

// EventStore implements an EventStore for MongoDB.
type EventStore struct {
	session         *mgo.Session
	dbPrefix        string
	colPrefix       string
	dataMarshaler   event.MarshalerFunc
	dataUnmarshaler event.UnmarshalerFunc
}

// NewEventStore creates a new EventStore.
func NewEventStore(url, dbPrefix string, colPrefix string, eventMarshaler event.MarshalerFunc, eventUnmarshaler event.UnmarshalerFunc) (*EventStore, error) {
	session, err := mgo.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("could not dial database: %v", err)
	}

	session.SetMode(mgo.Strong, true)
	session.SetSafe(&mgo.Safe{W: 1})

	return NewEventStoreWithSession(session, dbPrefix, colPrefix, eventMarshaler, eventUnmarshaler)
}

// NewEventStoreWithSession creates a new EventStore with a session.
func NewEventStoreWithSession(session *mgo.Session, dbPrefix string, colPrefix string, eventMarshaler event.MarshalerFunc, eventUnmarshaler event.UnmarshalerFunc) (*EventStore, error) {
	if session == nil {
		return nil, errors.New("no database session")
	}

	if eventMarshaler == nil {
		return nil, errors.New("no event marshaler")
	}
	if eventUnmarshaler == nil {
		return nil, errors.New("no event unmarshaler")
	}

	if dbPrefix == "" {
		dbPrefix = "default"
	}

	if dbPrefix == "" {
		colPrefix = "events"
	}

	s := &EventStore{
		session:         session,
		dbPrefix:        dbPrefix,
		colPrefix:       colPrefix,
		dataMarshaler:   eventMarshaler,
		dataUnmarshaler: eventUnmarshaler,
	}

	return s, nil
}

// Save save events to path.
func (s *EventStore) Save(ctx context.Context, path protoEvent.Path, events []event.Event) (concurrencyException bool, err error) {
	if len(events) == 0 {
		return false, errors.New("cannot save empty events")
	}
	if path.AggregateId == "" {
		return false, errors.New("cannot save events without AggregateId")
	}

	sess := s.session.Copy()
	defer sess.Close()

	ops := make([]txn.Op, 0, len(events))
	firstEvent := true
	version := events[0].Version()
	cname := Path2CName(path)
	for _, event := range events {
		if path.AggregateId != event.AggregateId() {
			return false, errors.New("cannot append event with no valid AggregateId")
		}

		if firstEvent {
			firstEvent = false
		} else {
			// Only accept events that apply to the correct aggregate version.
			if event.Version() != version+1 {
				return false, errors.New("cannot append unordered events")
			}
			version++
		}

		// Create the event record for the DB.
		e, err := makeDBEvent(ctx, path.AggregateId, event, s.dataMarshaler)
		if err != nil {
			return false, err
		}
		ops = append(ops, txn.Op{
			Id:     version,
			C:      cname,
			Assert: txn.DocMissing,
			Insert: e,
		})

	}

	runner := txn.NewRunner(sess.DB(s.DBName()).C(cname))
	err = runner.Run(ops, "", nil)

	if err != nil {
		return true, fmt.Errorf("cannot save events: %v", err)
	}
	return false, err
}

type eventUnmarshaler struct {
	e           dbEvent
	unmarshaler event.UnmarshalerFunc
}

func (eu eventUnmarshaler) Unmarshal(v interface{}) error {
	return eu.unmarshaler(eu.e.Data.Data, v)
}

func (eu eventUnmarshaler) Version() uint64 {
	return eu.e.Version
}

func (eu eventUnmarshaler) AggregateId() string {
	return eu.e.AggregateId
}

func (eu eventUnmarshaler) EventType() string {
	return eu.e.EventType
}

func processIterator(ctx context.Context, path protoEvent.Path, eh eventstore.Handler, dataUnmarshaler event.UnmarshalerFunc, iter *mgo.Iter) (int, error) {
	eu := eventUnmarshaler{unmarshaler: dataUnmarshaler}
	var version uint64
	firstIter := true

	numEvents := 0
	for iter.Next(&eu.e) {
		if firstIter {
			version = eu.e.Version
			firstIter = false
		} else {
			if version+1 != eu.e.Version {
				return -1, errors.New("invalid event version stored in eventstore")
			}
			version++
		}
		applied, err := eh.HandleEventFromStore(ctx, path, eu)
		if err != nil {
			return -1, fmt.Errorf("cannot load events %v", err)
		}
		if applied {
			numEvents++
		}

	}
	return numEvents, nil
}

// Load loads events from begining.
func (s *EventStore) Load(ctx context.Context, path protoEvent.Path, eh eventstore.Handler) (int, error) {
	if path.AggregateId == "" {
		return -1, errors.New("cannot load events without AggregateId")
	}
	sess := s.session.Copy()
	defer sess.Close()

	iter := sess.DB(s.DBName()).C(Path2CName(path)).Find(bson.M{"aggregateid": path.AggregateId}).Iter()

	numEvents, err := processIterator(ctx, path, eh, s.dataUnmarshaler, iter)
	errClose := iter.Close()
	if numEvents > 0 && err == nil {
		return numEvents, errClose
	}
	return numEvents, err
}

// LoadLastEvents loads last number of events from eventstore.
func (s *EventStore) LoadLastEvents(ctx context.Context, path protoEvent.Path, limit int, eh eventstore.Handler) (int, error) {
	if path.AggregateId == "" {
		return -1, errors.New("cannot load events without AggregateId")
	}
	if limit <= 0 {
		return -1, errors.New("cannot load last events with negative limit <= 0")
	}
	sess := s.session.Copy()
	defer sess.Close()

	iter := sess.DB(s.DBName()).C(Path2CName(path)).Find(bson.M{"aggregateid": path.AggregateId}).Sort("-_id").Limit(limit).Iter()

	events := make([]dbEvent, 0, limit)

	var e dbEvent
	for iter.Next(&e) {
		events = append([]dbEvent{e}, events...)
	}
	numEvents := 0
	firstIter := true
	if iter.Err() == nil {
		var version uint64
		for _, event := range events {
			if firstIter {
				version = event.Version
				firstIter = false
			} else {
				if version+1 != event.Version {
					return -1, errors.New("invalid event version stored in eventstore")
				}
				version++
			}
			ed := eventUnmarshaler{e: event, unmarshaler: s.dataUnmarshaler}
			applied, err := eh.HandleEventFromStore(ctx, path, ed)
			if err != nil {
				iter.Close()
				return -1, fmt.Errorf("cannot load last events %v", err)
			}
			if applied {
				numEvents++
			}
		}
	} else if iter.Err() == mgo.ErrNotFound {
		return 0, nil
	}

	return numEvents, iter.Close()
}

// LoadFromVersion loads greater or equal events than version.
func (s *EventStore) LoadFromVersion(ctx context.Context, path protoEvent.Path, version uint64, eh eventstore.Handler) (int, error) {
	if path.AggregateId == "" {
		return -1, errors.New("cannot load events without AggregateId")
	}
	sess := s.session.Copy()
	defer sess.Close()

	iter := sess.DB(s.DBName()).C(Path2CName(path)).Find(bson.M{"_id": bson.M{"$gte": version}}).Iter()

	numEvents, err := processIterator(ctx, path, eh, s.dataUnmarshaler, iter)
	errClose := iter.Close()
	if numEvents > 0 && err == nil {
		return numEvents, errClose
	}
	return numEvents, err
}

// ListPaths lists aggregate paths by provided path.
func (s *EventStore) ListPaths(ctx context.Context, path protoEvent.Path) ([]protoEvent.Path, error) {
	sess := s.session.Copy()
	defer sess.Close()
	names, err := sess.DB(s.DBName()).CollectionNames()
	if err != nil {
		return nil, fmt.Errorf("cannot get collection names: %v", err)
	}

	prefix := Path2CName(path)

	result := make([]protoEvent.Path, 0, 128)

	for _, n := range names {
		if strings.HasPrefix(n, prefix) && !strings.HasSuffix(n, ".stash") {
			result = append(result, CName2Path(n))
		}
	}
	return result, nil
}

// Clear clears the event storage.
func (s *EventStore) Clear(ctx context.Context) error {
	sess := s.session.Copy()
	defer sess.Close()
	names, err := sess.DB(s.DBName()).CollectionNames()
	if err != nil {
		return fmt.Errorf("cannot get collection names: %v", err)
	}

	prefix := Path2CName(protoEvent.Path{})

	for _, n := range names {
		if strings.HasPrefix(n, prefix) {
			if err := sess.DB(s.DBName()).C(n).DropCollection(); err != nil {
				return fmt.Errorf("cannot drop collection %v: %v", n, err)
			}
		}
	}
	return nil
}

// Close closes the database session.
func (s *EventStore) Close() {
	s.session.Close()
}

// DBName returns db name
func (s *EventStore) DBName() string {
	ns := "db"
	return s.dbPrefix + "_" + ns
}

// Path2CName appends the paths for create collection name.
func Path2CName(p protoEvent.Path) string {
	var buffer bytes.Buffer

	buffer.WriteString("events")

	for _, pa := range p.Path {
		buffer.WriteString("/")
		buffer.WriteString(pa)
	}

	buffer.WriteString("/")
	buffer.WriteString(p.AggregateId)

	return buffer.String()
}

func CName2Path(cname string) protoEvent.Path {
	p := strings.Split(cname, "/")
	return protoEvent.Path{
		Path:        p[1 : len(p)-1],
		AggregateId: p[len(p)-1],
	}
}

// dbEvent is the internal event record for the MongoDB event store used
// to save and load events from the DB.
type dbEvent struct {
	Data        bson.Binary `bson:"data,omitempty"`
	AggregateId string      `bson:"aggregateid"`
	Version     uint64      `bson:"_id"`
	EventType   string      `bson:"eventtype"`
}

// newDBEvent returns a new dbEvent for an event.
func makeDBEvent(ctx context.Context, aggregateID string, event event.Event, marshaler event.MarshalerFunc) (dbEvent, error) {
	// Marshal event data if there is any.
	raw, err := marshaler(event)
	if err != nil {
		return dbEvent{}, fmt.Errorf("cannot create db event: %v", err)
	}
	rawData := bson.Binary{Kind: 0, Data: raw}

	return dbEvent{
		Data:        rawData,
		AggregateId: aggregateID,
		Version:     event.Version(),
		EventType:   event.EventType(),
	}, nil
}
