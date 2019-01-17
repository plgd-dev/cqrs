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

type iterator struct {
	firstIter       bool
	version         uint64
	iter            *mgo.Iter
	dataUnmarshaler event.UnmarshalerFunc
	err             error
}

func (i *iterator) Next(e *event.EventUnmarshaler) bool {
	var event dbEvent

	if i.iter.Next(&event) {
		if i.firstIter {
			i.version = event.Version
			i.firstIter = false
		} else {
			if i.version+1 != event.Version {
				i.err = errors.New("invalid event version stored in eventstore")
				return false
			}
			i.version++
		}

		e.Version = event.Version
		e.AggregateId = event.AggregateId
		e.EventType = event.EventType
		e.Unmarshal = func(v interface{}) error {
			return i.dataUnmarshaler(event.Data.Data, v)
		}
		return true
	}
	return false
}

func (i *iterator) Err() error {
	if i.err == nil {
		return i.iter.Err()
	}
	return i.err
}

// Load loads events from begining.
func (s *EventStore) Load(ctx context.Context, path protoEvent.Path, eh eventstore.Handler) (int, error) {
	if path.AggregateId == "" {
		return -1, errors.New("cannot load events without AggregateId")
	}
	sess := s.session.Copy()
	defer sess.Close()

	iter := sess.DB(s.DBName()).C(Path2CName(path)).Find(bson.M{"aggregateid": path.AggregateId}).Iter()

	i := iterator{
		firstIter:       true,
		iter:            iter,
		dataUnmarshaler: s.dataUnmarshaler,
	}
	numEvents, err := eh.HandleEventFromStore(ctx, path, &i)

	errClose := iter.Close()
	if numEvents > 0 && err == nil {
		return numEvents, errClose
	}
	return numEvents, err
}

type lastIterator struct {
	firstIter       bool
	version         uint64
	idx             int
	events          []dbEvent
	dataUnmarshaler event.UnmarshalerFunc
	err             error
}

func (i *lastIterator) Next(e *event.EventUnmarshaler) bool {
	if i.idx < len(i.events) {
		if i.firstIter {
			i.version = i.events[i.idx].Version
			i.firstIter = false
		} else {
			if i.version+1 != i.events[i.idx].Version {
				i.err = errors.New("invalid event version stored in eventstore")
				return false
			}
			i.version++
		}

		e.Version = i.events[i.idx].Version
		e.AggregateId = i.events[i.idx].AggregateId
		e.EventType = i.events[i.idx].EventType
		idx := i.idx
		e.Unmarshal = func(v interface{}) error {
			return i.dataUnmarshaler(i.events[idx].Data.Data, v)
		}
		i.idx++
		return true
	}
	return false
}

func (i *lastIterator) Err() error {
	return i.err
}

// LoadLatest loads last number of events from eventstore.
func (s *EventStore) LoadLatest(ctx context.Context, path protoEvent.Path, count int, eh eventstore.Handler) (int, error) {
	if path.AggregateId == "" {
		return -1, errors.New("cannot load events without AggregateId")
	}
	if count <= 0 {
		return -1, errors.New("cannot load last events with negative limit <= 0")
	}
	sess := s.session.Copy()
	defer sess.Close()

	iter := sess.DB(s.DBName()).C(Path2CName(path)).Find(bson.M{"aggregateid": path.AggregateId}).Sort("-_id").Limit(count).Iter()

	events := make([]dbEvent, 0, count)

	var e dbEvent
	for iter.Next(&e) {
		events = append([]dbEvent{e}, events...)
	}
	err := iter.Close()
	if err != nil {
		return 0, fmt.Errorf("cannot load last events: %v", err)
	}

	i := lastIterator{
		firstIter:       true,
		events:          events,
		dataUnmarshaler: s.dataUnmarshaler,
	}

	return eh.HandleEventFromStore(ctx, path, &i)
}

// LoadFromVersion loads greater or equal events than version.
func (s *EventStore) LoadFromVersion(ctx context.Context, path protoEvent.Path, version uint64, eh eventstore.Handler) (int, error) {
	if path.AggregateId == "" {
		return -1, errors.New("cannot load events without AggregateId")
	}
	sess := s.session.Copy()
	defer sess.Close()

	iter := sess.DB(s.DBName()).C(Path2CName(path)).Find(bson.M{"_id": bson.M{"$gte": version}}).Iter()

	i := iterator{
		firstIter:       true,
		iter:            iter,
		dataUnmarshaler: s.dataUnmarshaler,
	}
	numEvents, err := eh.HandleEventFromStore(ctx, path, &i)

	errClose := iter.Close()
	if numEvents > 0 && err == nil {
		return numEvents, errClose
	}
	return numEvents, err
}

type pathIterator struct {
	idx   int
	paths []protoEvent.Path
}

func (i *pathIterator) Next(path *protoEvent.Path) bool {
	if i.idx < len(i.paths) {
		*path = i.paths[i.idx]
		i.idx++
		return true
	}
	return false
}

func (i *pathIterator) Err() error {
	return nil
}

// ListPaths lists aggregate paths by provided path.
func (s *EventStore) ListPaths(ctx context.Context, path protoEvent.Path, pathsHandler eventstore.PathsHandler) error {
	if pathsHandler == nil {
		return fmt.Errorf("invalid pathsHandler")
	}
	sess := s.session.Copy()
	defer sess.Close()
	names, err := sess.DB(s.DBName()).CollectionNames()
	if err != nil {
		return fmt.Errorf("cannot get collection names: %v", err)
	}

	prefix := Path2CName(path)

	paths := make([]protoEvent.Path, 0, 128)

	for _, n := range names {
		if strings.HasPrefix(n, prefix) && !strings.HasSuffix(n, ".stash") {
			paths = append(paths, CName2Path(n))
		}
	}

	i := pathIterator{
		paths: paths,
	}

	return pathsHandler.HandlePaths(ctx, &i)
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
