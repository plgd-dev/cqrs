package mongodb

import (
	"context"
	"fmt"
	"strconv"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/go-ocf/cqrs/eventstore"
	"github.com/go-ocf/cqrs/eventstore/maintenance"
	"go.mongodb.org/mongo-driver/bson"
)

const aggregateVersionsCName = "aggregateversions"

// dbEvent is the internal record for the MongoDB event store used to save and load
// the latest versions per aggregate (snapshot follows right after this version) from the DB.
type dbAggregateVersion struct {
	AggregateID string `bson:"aggregateIdKey"`
	ID          string `bson:"_id"`
	Version     uint64 `bson:"versionKey"`
}

func makeDbAggregateVersion(aggregateID string, version uint64) (dbAggregateVersion, error) {
	return dbAggregateVersion{
		AggregateID: aggregateID,
		Version:     version,
		ID:          aggregateID + "." + strconv.FormatUint(version, 10),
	}, nil
}

// Insert stores (or updates) the information about the latest snapshot version per aggregate into the DB
func (s *EventStore) Insert(ctx context.Context, task eventstore.VersionQuery) error {
	record, err := makeDbAggregateVersion(task.AggregateId, task.Version)
	if err != nil {
		return err
	}

	col := s.client.Database(s.DBName()).Collection(aggregateVersionsCName)

	opts := options.UpdateOptions{}
	opts.SetUpsert(true)

	res, err := col.UpdateOne(ctx,
		bson.M{
			"_id": record.ID,
			versionKey: bson.M{
				"$lt": record.Version,
			},
		},
		bson.M{
			"$set": record,
		},
		&opts,
	)
	if err != nil {
		if err == mongo.ErrNilDocument || IsDup(err) {
			// someone has already updated the store with a newer version
			return nil
		}
		return fmt.Errorf("db maintenance - could not upsert record with aggregate ID %v, version %d - %v", task.AggregateId, task.Version, err)
	}
	if res.UpsertedCount != 1 {
		return fmt.Errorf("db maintenance - could not upsert record with aggregate ID %v, version %d", task.AggregateId, task.Version)
	}
	return nil
}

type dbAggregateVersionIterator struct {
	iter *mongo.Cursor
}

func (i *dbAggregateVersionIterator) Next(ctx context.Context, task *eventstore.VersionQuery) bool {
	var dbRecord dbAggregateVersion

	if !i.iter.Next(ctx) {
		return false
	}

	err := i.iter.Decode(&dbRecord)
	if err != nil {
		return false
	}

	task.AggregateId = dbRecord.AggregateID
	task.Version = dbRecord.Version
	return true
}

func (i *dbAggregateVersionIterator) Err() error {
	return i.iter.Err()
}

// Query retrieves the latest snapshot version per aggregate for thw number of aggregates specified by 'limit'
func (s *EventStore) Query(ctx context.Context, limit int, taskHandler maintenance.TaskHandler) error {
	opts := options.FindOptions{}
	opts.SetLimit(int64(limit))
	iter, err := s.client.Database(s.DBName()).Collection(aggregateVersionsCName).Find(ctx, nil, &opts)
	if err == mongo.ErrNilDocument {
		return nil
	}
	if err != nil {
		return err
	}

	i := dbAggregateVersionIterator{
		iter: iter,
	}
	err = taskHandler.Handle(ctx, &i)

	errClose := iter.Close(ctx)
	if err == nil {
		return errClose
	}
	return err
}

// Remove deletes (the latest snapshot version) database record for a given aggregate ID
func (s *EventStore) Remove(ctx context.Context, task eventstore.VersionQuery) error {
	record, err := makeDbAggregateVersion(task.AggregateId, task.Version)
	if err != nil {
		return err
	}

	col := s.client.Database(s.DBName()).Collection(aggregateVersionsCName)

	res, err := col.DeleteOne(ctx, record)
	if err != nil {
		return err
	}
	if res.DeletedCount != 1 {
		return fmt.Errorf("db maintenance - could not remove record with given aggregate ID %s and/or version %d", task.AggregateId, task.Version)
	}

	return nil
}
