package mongodb

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"

	"github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventstore"
)

const eventCName = "events"
const snapshotCName = "snapshots"

const aggregateIdKey = "aggregateid"
const groupIdKey = "groupid"
const versionKey = "version"

var snapshotsQueryIndex = bson.D{
	{groupIdKey, 1},
	{aggregateIdKey, 1},
}

var snapshotsQueryGroupIdIndex = bson.D{
	{groupIdKey, 1},
}

var eventsQueryIndex = bson.D{
	{versionKey, 1},
	{aggregateIdKey, 1},
	{groupIdKey, 1},
}
var eventsQueryGroupIdIndex = bson.D{
	{versionKey, 1},
	{groupIdKey, 1},
}
var eventsQueryAggregateIdIndex = bson.D{
	{versionKey, 1},
	{aggregateIdKey, 1},
}

type signOperator string

const (
	signOperator_gte signOperator = "$gte"
	signOperator_lt  signOperator = "$lt"
)

type LogDebugfFunc func(fmt string, args ...interface{})

// EventStore implements an EventStore for MongoDB.
type EventStore struct {
	client          *mongo.Client
	goroutinePoolGo eventstore.GoroutinePoolGoFunc
	LogDebugfFunc   LogDebugfFunc
	dbPrefix        string
	colPrefix       string
	batchSize       int
	dataMarshaler   event.MarshalerFunc
	dataUnmarshaler event.UnmarshalerFunc
}

// NewEventStore creates a new EventStore.
func NewEventStore(ctx context.Context, host, dbPrefix string, colPrefix string, batchSize int, goroutinePoolGo eventstore.GoroutinePoolGoFunc, eventMarshaler event.MarshalerFunc, eventUnmarshaler event.UnmarshalerFunc, LogDebugfFunc LogDebugfFunc, opts ...*options.ClientOptions) (*EventStore, error) {
	newOpts := []*options.ClientOptions{options.Client().ApplyURI("mongodb://" + host)}
	newOpts = append(newOpts, opts...)
	client, err := mongo.Connect(ctx, newOpts...)
	if err != nil {
		return nil, fmt.Errorf("could not dial database: %v", err)
	}
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, fmt.Errorf("could not dial database: %v", err)
	}

	return NewEventStoreWithClient(ctx, client, dbPrefix, colPrefix, batchSize, goroutinePoolGo, eventMarshaler, eventUnmarshaler, LogDebugfFunc)
}

// NewEventStoreWithClient creates a new EventStore with a session.
func NewEventStoreWithClient(ctx context.Context, client *mongo.Client, dbPrefix string, colPrefix string, batchSize int, goroutinePoolGo eventstore.GoroutinePoolGoFunc, eventMarshaler event.MarshalerFunc, eventUnmarshaler event.UnmarshalerFunc, LogDebugfFunc LogDebugfFunc) (*EventStore, error) {
	if client == nil {
		return nil, errors.New("invalid client")
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

	if batchSize < 1 {
		batchSize = 128
	}

	if LogDebugfFunc == nil {
		LogDebugfFunc = func(fmt string, args ...interface{}) {}
	}

	s := &EventStore{
		goroutinePoolGo: goroutinePoolGo,
		client:          client,
		dbPrefix:        dbPrefix,
		colPrefix:       colPrefix,
		dataMarshaler:   eventMarshaler,
		dataUnmarshaler: eventUnmarshaler,
		batchSize:       batchSize,
		LogDebugfFunc:   LogDebugfFunc,
	}

	colEv := s.client.Database(s.DBName()).Collection(eventCName)
	err := ensureIndex(ctx, colEv, eventsQueryIndex, eventsQueryGroupIdIndex, eventsQueryAggregateIdIndex)
	if err != nil {
		return nil, fmt.Errorf("cannot save events: %v", err)
	}
	colSn := s.client.Database(s.DBName()).Collection(snapshotCName)
	err = ensureIndex(ctx, colSn, snapshotsQueryIndex, snapshotsQueryGroupIdIndex)
	if err != nil {
		return nil, fmt.Errorf("cannot save snapshot query: %v", err)
	}

	return s, nil
}

// IsDup check it error is duplicate
func IsDup(err error) bool {
	// Besides being handy, helps with MongoDB bugs SERVER-7164 and SERVER-11493.
	// What follows makes me sad. Hopefully conventions will be more clear over time.
	switch e := err.(type) {
	case mongo.CommandError:
		return e.Code == 11000 || e.Code == 11001 || e.Code == 12582 || e.Code == 16460 && strings.Contains(e.Message, " E11000 ")
	case mongo.WriteError:
		return e.Code == 11000 || e.Code == 11001 || e.Code == 12582
	case mongo.WriteException:
		isDup := true
		for _, werr := range e.WriteErrors {
			if !IsDup(werr) {
				isDup = false
			}
		}
		return isDup
	}
	return false
}

func (s *EventStore) saveEvent(ctx context.Context, col *mongo.Collection, groupId string, aggregateId string, event event.Event) (concurrencyException bool, err error) {
	e, err := makeDBEvent(groupId, aggregateId, event, s.dataMarshaler)
	if err != nil {
		return false, err
	}

	if _, err := col.InsertOne(ctx, e); err != nil {
		if IsDup(err) {
			return true, nil
		}
		return false, fmt.Errorf("cannot save events: %v", err)
	}
	return false, nil
}

func (s *EventStore) saveEvents(ctx context.Context, col *mongo.Collection, groupId, aggregateId string, events []event.Event) (concurrencyException bool, err error) {
	firstEvent := true
	version := events[0].Version()
	ops := make([]interface{}, 0, len(events))
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
		ops = append(ops, e)
	}

	if _, err := col.InsertMany(ctx, ops); err != nil {
		if IsDup(err) {
			return true, nil
		}
		return false, fmt.Errorf("cannot save events: %v", err)
	}
	return false, err
}

type index struct {
	Key  map[string]int
	NS   string
	Name string
}

func ensureIndex(ctx context.Context, col *mongo.Collection, indexes ...bson.D) error {
	for _, keys := range indexes {
		opts := options.Index()
		opts.SetBackground(false)
		index := mongo.IndexModel{
			Keys:    keys,
			Options: opts,
		}

		_, err := col.Indexes().CreateOne(ctx, index)
		if err != nil {
			if strings.HasPrefix(err.Error(), "(IndexKeySpecsConflict)") {
				//index already exist, just skip error and continue
				continue
			}
			return fmt.Errorf("cannot ensure indexes for eventstore: %v", err)
		}
	}
	return nil
}

// Save save events to path.
func (s *EventStore) Save(ctx context.Context, groupId, aggregateId string, events []event.Event) (concurrencyException bool, err error) {
	s.LogDebugfFunc("mongodb.Evenstore.Save start")
	t := time.Now()
	defer func() {
		s.LogDebugfFunc("mongodb.Evenstore.Save takes %v", time.Since(t))
	}()

	if len(events) == 0 {
		return false, errors.New("cannot save empty events")
	}
	if aggregateId == "" {
		return false, errors.New("cannot save events without AggregateId")
	}

	if events[0].Version() == 0 {
		concurrencyException, err = s.SaveSnapshotQuery(ctx, groupId, aggregateId, 0)
		if err != nil {
			return false, fmt.Errorf("cannot save events without snapshot query for version 0: %v", err)
		}
		if concurrencyException {
			return concurrencyException, nil
		}
	}

	col := s.client.Database(s.DBName()).Collection(eventCName)
	/*
		err = ensureIndex(ctx, col, eventsQueryIndex, eventsQueryGroupIdIndex, eventsQueryAggregateIdIndex)
		if err != nil {
			return false, fmt.Errorf("cannot save events: %v", err)
		}
	*/

	if len(events) > 1 {
		return s.saveEvents(ctx, col, groupId, aggregateId, events)
	}
	return s.saveEvent(ctx, col, groupId, aggregateId, events[0])
}

func (s *EventStore) SaveSnapshot(ctx context.Context, groupId string, aggregateId string, ev event.Event) (concurrencyException bool, err error) {
	concurrencyException, err = s.Save(ctx, groupId, aggregateId, []event.Event{ev})
	if !concurrencyException && err == nil {
		return s.SaveSnapshotQuery(ctx, groupId, aggregateId, ev.Version())
	}
	return concurrencyException, err
}

type iterator struct {
	iter            *mongo.Cursor
	dataUnmarshaler event.UnmarshalerFunc
	LogDebugfFunc   LogDebugfFunc
}

func (i *iterator) Next(ctx context.Context, e *event.EventUnmarshaler) bool {
	var event dbEvent

	if !i.iter.Next(ctx) {
		return false
	}

	err := i.iter.Decode(&event)
	if err != nil {
		return false
	}

	i.LogDebugfFunc("mongodb.iterator.next: GroupId %v: AggregateId %v: Version %v, EvenType %v", event.GroupId, event.AggregateId, event.Version, event.EventType)

	e.Version = event.Version
	e.AggregateId = event.AggregateId
	e.EventType = event.EventType
	e.GroupId = event.GroupId
	data := event.Data
	e.Unmarshal = func(v interface{}) error {
		return i.dataUnmarshaler(data, v)
	}
	return true
}

func (i *iterator) Err() error {
	return i.iter.Err()
}

func versionQueriesToMgoQuery(queries []eventstore.VersionQuery, op signOperator) (bson.M, error) {
	orQueries := make([]bson.M, 0, 32)

	if len(queries) == 0 {
		return bson.M{}, fmt.Errorf("empty []eventstore.VersionQuery")
	}

	for _, q := range queries {
		if q.AggregateId == "" {
			return bson.M{}, fmt.Errorf("invalid VersionQuery.AggregateId")
		}
		orQueries = append(orQueries, versionQueryToMgoQuery(q, op))
	}

	return bson.M{"$or": orQueries}, nil
}

func versionQueryToMgoQuery(query eventstore.VersionQuery, op signOperator) bson.M {
	andQueries := make([]bson.M, 0, 2)
	andQueries = append(andQueries, bson.M{versionKey: bson.M{string(op): query.Version}})
	andQueries = append(andQueries, bson.M{aggregateIdKey: query.AggregateId})
	return bson.M{"$and": andQueries}
}

type loader struct {
	store        *EventStore
	eventHandler event.Handler
}

func (l *loader) QueryHandle(ctx context.Context, iter *queryIterator) error {
	var query eventstore.VersionQuery
	queries := make([]eventstore.VersionQuery, 0, 128)
	var errors []error

	for iter.Next(ctx, &query) {
		queries = append(queries, query)
		if len(queries) >= l.store.batchSize {
			err := l.store.LoadFromVersion(ctx, queries, l.eventHandler)
			if err != nil {
				errors = append(errors, fmt.Errorf("cannot load events to eventstore model: %v", err))
			}
			queries = queries[:0]
		}
	}
	if len(errors) > 0 {
		return fmt.Errorf("loader cannot load events: %v", errors)
	}

	if iter.Err() != nil {
		return iter.Err()
	}

	if len(queries) > 0 {
		return l.store.LoadFromVersion(ctx, queries, l.eventHandler)
	}

	return nil
}

func (l *loader) QueryHandlePool(ctx context.Context, iter *queryIterator) error {
	var query eventstore.VersionQuery
	queries := make([]eventstore.VersionQuery, 0, 128)
	var wg sync.WaitGroup

	var errors []error
	var errorsLock sync.Mutex

	for iter.Next(ctx, &query) {
		queries = append(queries, query)
		if len(queries) >= l.store.batchSize {
			wg.Add(1)
			l.store.LogDebugfFunc("mongodb:loader:QueryHandlePool:newTask")
			tmp := queries
			err := l.store.goroutinePoolGo(func() {
				defer wg.Done()
				l.store.LogDebugfFunc("mongodb:loader:QueryHandlePool:task:LoadFromVersion:start")
				err := l.store.LoadFromVersion(ctx, tmp, l.eventHandler)
				l.store.LogDebugfFunc("mongodb:loader:QueryHandlePool:task:LoadFromVersion:done")
				if err != nil {
					errorsLock.Lock()
					defer errorsLock.Unlock()
					errors = append(errors, fmt.Errorf("cannot load events to eventstore model: %v", err))
				}
				l.store.LogDebugfFunc("mongodb:loader:QueryHandlePool:doneTask")
			})
			if err != nil {
				wg.Done()
				errorsLock.Lock()
				errors = append(errors, fmt.Errorf("cannot submit task to load events to eventstore model: %v", err))
				errorsLock.Unlock()
				break
			}
			queries = make([]eventstore.VersionQuery, 0, 128)
		}
	}
	wg.Wait()
	if len(errors) > 0 {
		return fmt.Errorf("loader cannot load events: %v", errors)
	}

	if iter.Err() != nil {
		return iter.Err()
	}
	if len(queries) > 0 {
		return l.store.LoadFromVersion(ctx, queries, l.eventHandler)
	}

	return nil
}

// LoadUpToVersion loads aggragates events up to a specific version.
func (s *EventStore) LoadUpToVersion(ctx context.Context, queries []eventstore.VersionQuery, eh event.Handler) error {
	s.LogDebugfFunc("mongodb.Eventstore.LoadUpToVersion start")
	t := time.Now()
	defer func() {
		s.LogDebugfFunc("mongodb.Eventstore.LoadUpToVersion takes %v", time.Since(t))
	}()

	q, err := versionQueriesToMgoQuery(queries, signOperator_lt)
	if err != nil {
		return fmt.Errorf("cannot load events up to version: %v", err)
	}

	return s.loadMgoQuery(ctx, eh, q)
}

// LoadFromVersion loads aggragates events from version.
func (s *EventStore) LoadFromVersion(ctx context.Context, queries []eventstore.VersionQuery, eh event.Handler) error {
	s.LogDebugfFunc("mongodb.Evenstore.LoadFromVersion start")
	t := time.Now()
	defer func() {
		s.LogDebugfFunc("mongodb.Evenstore.LoadFromVersion takes %v", time.Since(t))
	}()

	q, err := versionQueriesToMgoQuery(queries, signOperator_gte)
	if err != nil {
		return fmt.Errorf("cannot load events from version: %v", err)
	}

	return s.loadMgoQuery(ctx, eh, q)
}

func (s *EventStore) loadMgoQuery(ctx context.Context, eh event.Handler, mgoQuery bson.M) error {
	opts := options.FindOptions{}
	opts.SetHint(eventsQueryAggregateIdIndex)
	iter, err := s.client.Database(s.DBName()).Collection(eventCName).Find(ctx, mgoQuery, &opts)
	if err == mongo.ErrNilDocument {
		return nil
	}
	if err != nil {
		return err
	}

	i := iterator{
		iter:            iter,
		dataUnmarshaler: s.dataUnmarshaler,
		LogDebugfFunc:   s.LogDebugfFunc,
	}
	err = eh.Handle(ctx, &i)

	errClose := iter.Close(ctx)
	if err == nil {
		return errClose
	}
	return err
}

// Load loads events from begining.
func (s *EventStore) LoadFromSnapshot(ctx context.Context, queries []eventstore.SnapshotQuery, eventHandler event.Handler) error {
	s.LogDebugfFunc("mongodb.Evenstore.LoadFromSnapshot start")
	t := time.Now()
	defer func() {
		s.LogDebugfFunc("mongodb.Evenstore.LoadFromSnapshot takes %v", time.Since(t))
	}()
	return s.LoadSnapshotQueries(ctx, queries, &loader{
		store:        s,
		eventHandler: eventHandler,
	})
}

// DBName returns db name
func (s *EventStore) DBName() string {
	ns := "db"
	return s.dbPrefix + "_" + ns
}

// Clear clears the event storage.
func (s *EventStore) Clear(ctx context.Context) error {
	var errors []error
	s.client.Database(s.DBName()).Collection(eventCName).Indexes().DropAll(ctx)
	if err := s.client.Database(s.DBName()).Collection(eventCName).Drop(ctx); err != nil {
		errors = append(errors, err)
	}
	s.client.Database(s.DBName()).Collection(snapshotCName).Indexes().DropAll(ctx)
	if err := s.client.Database(s.DBName()).Collection(snapshotCName).Drop(ctx); err != nil {
		errors = append(errors, err)
	}
	if len(errors) > 0 {
		return fmt.Errorf("cannot clear: %v", errors)
	}

	return nil
}

// Close closes the database session.
func (s *EventStore) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

// dbEvent is the internal event record for the MongoDB event store used
// to save and load events from the DB.
type dbEvent struct {
	Data        []byte `bson:"data,omitempty"`
	AggregateId string `bson:aggregateIdKey`
	Id          string `bson:"_id"`
	Version     uint64 `bson:versionKey`
	EventType   string `bson:"eventtype"`
	GroupId     string `bson:groupIdKey`
}

// newDBEvent returns a new dbEvent for an event.
func makeDBEvent(groupId, aggregateId string, event event.Event, marshaler event.MarshalerFunc) (dbEvent, error) {
	// Marshal event data if there is any.
	raw, err := marshaler(event)
	if err != nil {
		return dbEvent{}, fmt.Errorf("cannot create db event: %v", err)
	}

	return dbEvent{
		Data:        raw,
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

func (s *EventStore) SaveSnapshotQuery(ctx context.Context, groupId, aggregateId string, version uint64) (concurrencyException bool, err error) {
	s.LogDebugfFunc("mongodb.Evenstore.SaveSnapshotQuery start")
	t := time.Now()
	defer func() {
		s.LogDebugfFunc("mongodb.Evenstore.SaveSnapshotQuery takes %v", time.Since(t))
	}()

	if aggregateId == "" {
		return false, fmt.Errorf("cannot save snapshot query: invalid query.AggregateId")
	}

	sbSnap := makeDBSnapshot(groupId, aggregateId, version)
	col := s.client.Database(s.DBName()).Collection(snapshotCName)
	/*
		err = ensureIndex(ctx, col, snapshotsQueryIndex, snapshotsQueryGroupIdIndex)
		if err != nil {
			return false, fmt.Errorf("cannot save snapshot query: %v", err)
		}
	*/
	if version == 0 {
		_, err := col.InsertOne(ctx, sbSnap)
		if err != nil && IsDup(err) {
			// someone update store newer snapshot
			return true, nil
		}
		return false, err
	}

	if _, err = col.UpdateOne(ctx,
		bson.M{
			"_id": sbSnap.Id,
			versionKey: bson.M{
				"$lt": sbSnap.Version,
			},
		},
		bson.M{
			"$set": sbSnap,
		},
	); err != nil {
		if err == mongo.ErrNilDocument || IsDup(err) {
			// someone update store newer snapshot
			return true, nil
		}
		return false, fmt.Errorf("cannot save snapshot query: %v", err)
	}
	return false, nil
}

func snapshotQueriesToMgoQuery(queries []eventstore.SnapshotQuery) bson.M {
	orQueries := make([]bson.M, 0, 32)

	for _, q := range queries {
		andQueries := make([]bson.M, 0, 4)
		if q.AggregateId != "" {
			andQueries = append(andQueries, bson.M{aggregateIdKey: q.AggregateId})
		}
		if q.AggregateId == "" && q.GroupId != "" {
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
	iter *mongo.Cursor
}

func (i *queryIterator) Next(ctx context.Context, q *eventstore.VersionQuery) bool {
	var query dbSnapshot

	if !i.iter.Next(ctx) {
		return false
	}

	err := i.iter.Decode(&query)
	if err != nil {
		return false
	}

	q.Version = query.Version
	q.AggregateId = query.AggregateId
	return true
}

func (i *queryIterator) Err() error {
	return i.iter.Err()
}

func (s *EventStore) LoadSnapshotQueries(ctx context.Context, queries []eventstore.SnapshotQuery, qh *loader) error {
	s.LogDebugfFunc("mongodb.Evenstore.LoadSnapshotQueries start")
	t := time.Now()
	defer func() {
		s.LogDebugfFunc("mongodb.Evenstore.LoadSnapshotQueries takes %v", time.Since(t))
	}()

	iter, err := s.client.Database(s.DBName()).Collection(snapshotCName).Find(ctx, snapshotQueriesToMgoQuery(queries))
	if err == mongo.ErrNilDocument {
		return nil
	}
	if err != nil {
		return err
	}
	if s.goroutinePoolGo != nil {
		err = qh.QueryHandlePool(ctx, &queryIterator{iter})
	} else {
		err = qh.QueryHandle(ctx, &queryIterator{iter})
	}
	errClose := iter.Close(ctx)
	if err == nil {
		return errClose
	}
	return err
}

func (s *EventStore) RemoveUpToVersion(ctx context.Context, queries []eventstore.VersionQuery) error {
	return nil
}
