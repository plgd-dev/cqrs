package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/globalsign/mgo/txn"

	"github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventstore"
)

const eventCName = "events"
const snapshotCName = "snapshots"

const aggregateIdKey = "aggregateid"
const groupIdKey = "groupid"
const versionKey = "version"

var snapshotsQueryIndex = []string{groupIdKey, aggregateIdKey}
var snapshotsQueryGroupIdIndex = []string{groupIdKey}

var eventsQueryIndex = []string{versionKey, aggregateIdKey, groupIdKey}
var eventsQueryGroupIdIndex = []string{versionKey, groupIdKey}

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

func (s *EventStore) saveEvent(col *mgo.Collection, groupId string, aggregateId string, event event.Event) (concurrencyException bool, err error) {
	e, err := makeDBEvent(groupId, aggregateId, event, s.dataMarshaler)
	if err != nil {
		return false, err
	}
	if err := col.Insert(e); err != nil {
		if mgo.IsDup(err) {
			return true, fmt.Errorf("cannot save events - concurrency exception: %v", err)
		}
		return false, fmt.Errorf("cannot save events: %v", err)
	}
	return false, nil
}

func (s *EventStore) saveEvents(col *mgo.Collection, groupId, aggregateId string, events []event.Event) (concurrencyException bool, err error) {
	firstEvent := true
	version := events[0].Version()
	ops := make([]txn.Op, 0, len(events))
	for _, event := range events {
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
		e, err := makeDBEvent(groupId, aggregateId, event, s.dataMarshaler)
		if err != nil {
			return false, err
		}
		ops = append(ops, txn.Op{
			Id:     e.Id,
			C:      eventCName,
			Assert: txn.DocMissing,
			Insert: e,
		})

	}

	runner := txn.NewRunner(col)
	err = runner.Run(ops, "", nil)

	if err != nil {
		return true, fmt.Errorf("cannot save events: %v", err)
	}
	return false, err
}

func ensureIndex(col *mgo.Collection, indexes ...[]string) error {

	for _, key := range indexes {
		index := mgo.Index{
			Key:        key,
			Background: true,
		}
		err := col.EnsureIndex(index)
		if err != nil {
			return fmt.Errorf("cannot ensure indexes for eventstore: %v", err)
		}
	}
	return nil
}

// Save save events to path.
func (s *EventStore) Save(ctx context.Context, groupId, aggregateId string, events []event.Event) (concurrencyException bool, err error) {
	sess := s.session.Copy()
	defer sess.Close()

	if len(events) == 0 {
		return false, errors.New("cannot save empty events")
	}
	if aggregateId == "" {
		return false, errors.New("cannot save events without AggregateId")
	}

	if events[0].Version() == 0 {
		err := s.SaveSnapshotQuery(ctx, groupId, aggregateId, 0)
		if err != nil {
			return false, fmt.Errorf("cannot save events without snapshot query for version 0: %v", err)
		}
	}

	col := sess.DB(s.DBName()).C(eventCName)

	err = ensureIndex(col, eventsQueryIndex, eventsQueryGroupIdIndex)
	if err != nil {
		return false, fmt.Errorf("cannot save events: %v", err)
	}

	if len(events) > 1 {
		return s.saveEvents(col, groupId, aggregateId, events)
	}
	return s.saveEvent(col, groupId, aggregateId, events[0])
}

type iterator struct {
	iter            *mgo.Iter
	dataUnmarshaler event.UnmarshalerFunc
}

func (i *iterator) Next(ctx context.Context, e *event.EventUnmarshaler) bool {
	var event dbEvent

	if !i.iter.Next(&event) {
		return false
	}

	e.Version = event.Version
	e.AggregateId = event.AggregateId
	e.EventType = event.EventType
	e.GroupId = event.GroupId
	e.Unmarshal = func(v interface{}) error {
		return i.dataUnmarshaler(event.Data.Data, v)
	}
	return true
}

func (i *iterator) Err() error {
	return i.iter.Err()
}

func eventQueriesToMgoQuery(queries []eventstore.Query) bson.M {
	orQueries := make([]bson.M, 0, 32)

	for _, q := range queries {
		andQueries := make([]bson.M, 0, 4)
		andQueries = append(andQueries, bson.M{versionKey: bson.M{"$gte": q.Version}})
		if q.AggregateId != "" {
			andQueries = append(andQueries, bson.M{aggregateIdKey: q.AggregateId})
		}
		if q.GroupId != "" {
			andQueries = append(andQueries, bson.M{groupIdKey: q.GroupId})
		}
		orQueries = append(orQueries, bson.M{"$and": andQueries})
	}

	if len(orQueries) > 0 {
		return bson.M{"$or": orQueries}
	}
	return bson.M{versionKey: bson.M{"$gte": 0}}
}

// Load loads events from begining.
func (s *EventStore) Load(ctx context.Context, queries []eventstore.Query, eh event.Handler) error {
	sess := s.session.Copy()
	defer sess.Close()

	iter := sess.DB(s.DBName()).C(eventCName).Find(eventQueriesToMgoQuery(queries)).Iter()

	i := iterator{
		iter:            iter,
		dataUnmarshaler: s.dataUnmarshaler,
	}
	err := eh.Handle(ctx, &i)

	errClose := iter.Close()
	if err == nil {
		return errClose
	}
	return err
}

// DBName returns db name
func (s *EventStore) DBName() string {
	ns := "db"
	return s.dbPrefix + "_" + ns
}

// Clear clears the event storage.
func (s *EventStore) Clear(ctx context.Context) error {
	sess := s.session.Copy()
	defer sess.Close()

	var errors []error
	if err := sess.DB(s.DBName()).C(eventCName).DropCollection(); err != nil {
		errors = append(errors, err)
	}
	if err := sess.DB(s.DBName()).C(snapshotCName).DropCollection(); err != nil {
		errors = append(errors, err)
	}
	if len(errors) > 0 {
		return fmt.Errorf("cannot clear: %v", errors)
	}

	return nil
}

// Close closes the database session.
func (s *EventStore) Close() {
	s.session.Close()
}

// dbEvent is the internal event record for the MongoDB event store used
// to save and load events from the DB.
type dbEvent struct {
	Data        bson.Binary `bson:"data,omitempty"`
	AggregateId string      `bson:aggregateIdKey`
	Id          string      `bson:"_id"`
	Version     uint64      `bson:versionKey`
	EventType   string      `bson:"eventtype"`
	GroupId     string      `bson:groupIdKey`
}

// newDBEvent returns a new dbEvent for an event.
func makeDBEvent(groupId, aggregateId string, event event.Event, marshaler event.MarshalerFunc) (dbEvent, error) {
	// Marshal event data if there is any.
	raw, err := marshaler(event)
	if err != nil {
		return dbEvent{}, fmt.Errorf("cannot create db event: %v", err)
	}
	rawData := bson.Binary{Kind: 0, Data: raw}

	return dbEvent{
		Data:        rawData,
		AggregateId: aggregateId,
		Version:     event.Version(),
		EventType:   event.EventType(),
		GroupId:     groupId,
		Id:          groupId + "." + aggregateId + "." + strconv.FormatUint(event.Version(), 10),
	}, nil
}

type dbSnapshot struct {
	Id          string `bson:"_id"`
	GroupId     string `bson:groupIdKey`
	AggregateId string `bson:aggregateIdKey`
	Version     uint64 `bson:versionKey`
}

// newDBEvent returns a new dbEvent for an event.
func makeDBSnapshot(groupId, aggregateId string, version uint64) dbSnapshot {
	return dbSnapshot{
		Id:          groupId + "." + aggregateId,
		GroupId:     groupId,
		AggregateId: aggregateId,
		Version:     version,
	}
}

func (s *EventStore) SaveSnapshotQuery(ctx context.Context, groupId, aggregateId string, version uint64) error {
	sess := s.session.Copy()
	defer sess.Close()

	if aggregateId == "" {
		return fmt.Errorf("cannot save snapshot query: invalid query.AggregateId")
	}

	sbSnap := makeDBSnapshot(groupId, aggregateId, version)
	col := sess.DB(s.DBName()).C(snapshotCName)

	err := ensureIndex(col, snapshotsQueryIndex, snapshotsQueryGroupIdIndex)
	if err != nil {
		return fmt.Errorf("cannot save snapshot query: %v", err)
	}

	if _, err := col.Upsert(
		bson.M{
			"_id": sbSnap.Id,
			versionKey: bson.M{
				"$lt": sbSnap.Version,
			},
		},
		sbSnap,
	); err != nil {
		if err == mgo.ErrNotFound {
			// someone update store newer snapshot
			return nil
		}
		return fmt.Errorf("cannot save snapshot query: %v", err)
	}
	return nil
}

func snapshotQueriesToMgoQuery(queries []eventstore.Query) bson.M {
	orQueries := make([]bson.M, 0, 32)

	for _, q := range queries {
		andQueries := make([]bson.M, 0, 4)
		if q.AggregateId != "" {
			andQueries = append(andQueries, bson.M{aggregateIdKey: q.AggregateId})
		}
		if q.GroupId != "" {
			andQueries = append(andQueries, bson.M{groupIdKey: q.GroupId})
		}
		orQueries = append(orQueries, bson.M{"$and": andQueries})
	}

	if len(orQueries) > 0 {
		return bson.M{"$or": orQueries}
	}
	return bson.M{}
}

type queryIterator struct {
	iter *mgo.Iter
}

func (i *queryIterator) Next(ctx context.Context, q *eventstore.Query) bool {
	var query dbSnapshot

	if !i.iter.Next(&query) {
		return false
	}

	q.Version = query.Version
	q.AggregateId = query.AggregateId
	q.GroupId = query.GroupId
	return true
}

func (i *queryIterator) Err() error {
	return i.iter.Err()
}

func (s *EventStore) LoadSnapshotQueries(ctx context.Context, queries []eventstore.Query, qh eventstore.QueryHandler) error {
	sess := s.session.Copy()
	defer sess.Close()

	iter := sess.DB(s.DBName()).C(snapshotCName).Find(snapshotQueriesToMgoQuery(queries)).Iter()

	err := qh.QueryHandle(ctx, &queryIterator{iter})
	errClose := iter.Close()
	if err == nil {
		return errClose
	}
	return err
}
