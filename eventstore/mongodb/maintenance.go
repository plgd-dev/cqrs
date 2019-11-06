package mongodb

import (
	"context"
	"fmt"
	"strconv"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/go-ocf/cqrs/eventstore/maintenance"
	"go.mongodb.org/mongo-driver/bson"
)

const maintenanceCName = "maintenance"

func makeDbAggregateVersion(task maintenance.Task) bson.M {
	return bson.M{
		aggregateIdKey: task.AggregateID,
		versionKey:     task.Version,
		idKey:          getID(task),
	}
}

func getID(task maintenance.Task) string {
	return task.AggregateID + "." + strconv.FormatUint(task.Version, 10)
}

// Insert stores (or updates) the information about the latest snapshot version per aggregate into the DB
func (s *EventStore) Insert(ctx context.Context, task maintenance.Task) error {
	record := makeDbAggregateVersion(task)

	col := s.client.Database(s.DBName()).Collection(maintenanceCName)

	opts := options.UpdateOptions{}
	opts.SetUpsert(true)

	res, err := col.UpdateOne(ctx,
		bson.M{
			idKey: getID(task),
			versionKey: bson.M{
				"$lt": task.Version,
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
		return fmt.Errorf("db maintenance - could not upsert record with aggregate ID %v, version %d - %v", task.AggregateID, task.Version, err)
	}
	if res.UpsertedCount != 1 {
		return fmt.Errorf("db maintenance - could not upsert record with aggregate ID %v, version %d", task.AggregateID, task.Version)
	}
	return nil
}

type dbAggregateVersionIterator struct {
	iter *mongo.Cursor
}

func (i *dbAggregateVersionIterator) Next(ctx context.Context, task *maintenance.Task) bool {
	var dbRecord bson.M

	if !i.iter.Next(ctx) {
		return false
	}

	err := i.iter.Decode(&dbRecord)
	if err != nil {
		return false
	}

	task.AggregateID = dbRecord[aggregateIdKey].(string)
	task.Version = dbRecord[versionKey].(uint64)
	return true
}

func (i *dbAggregateVersionIterator) Err() error {
	return i.iter.Err()
}

// Query retrieves the latest snapshot version per aggregate for thw number of aggregates specified by 'limit'
func (s *EventStore) Query(ctx context.Context, limit int, taskHandler maintenance.TaskHandler) error {
	opts := options.FindOptions{}
	opts.SetLimit(int64(limit))
	iter, err := s.client.Database(s.DBName()).Collection(maintenanceCName).Find(ctx, nil, &opts)
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
func (s *EventStore) Remove(ctx context.Context, task maintenance.Task) error {
	record := makeDbAggregateVersion(task)

	col := s.client.Database(s.DBName()).Collection(maintenanceCName)

	res, err := col.DeleteOne(ctx, record)
	if err != nil {
		return err
	}
	if res.DeletedCount != 1 {
		return fmt.Errorf("db maintenance - could not remove record with given aggregate ID %s and/or version %d", task.AggregateID, task.Version)
	}

	return nil
}
