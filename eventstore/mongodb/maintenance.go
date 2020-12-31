package mongodb

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/plgd-dev/cqrs/eventstore/maintenance"
	"go.mongodb.org/mongo-driver/bson"
)

const maintenanceCName = "maintenance"

func makeDbAggregateVersion(task maintenance.Task) bson.M {
	return bson.M{
		aggregateIDKey: task.AggregateID,
		versionKey:     task.Version,
		idKey:          getID(task),
	}
}

func getID(task maintenance.Task) string {
	return task.AggregateID
}

// Insert stores (or updates) the information about the latest snapshot version per aggregate into the DB
func (s *EventStore) Insert(ctx context.Context, task maintenance.Task) error {
	if task.AggregateID == "" || task.Version < 0 {
		return errors.New("could not insert record - aggregate ID and/or version cannot be empty")
	}

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
			return fmt.Errorf("could not insert record with aggregate ID %v, version %d - version is outdated - %w", task.AggregateID, task.Version, err)
		}
		return fmt.Errorf("could not insert record with aggregate ID %v, version %d - %w", task.AggregateID, task.Version, err)
	}

	if res.UpsertedCount != 1 && res.ModifiedCount != 1 {
		return fmt.Errorf("could not insert record with aggregate ID %v, version %d", task.AggregateID, task.Version)
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

	task.AggregateID = dbRecord[aggregateIDKey].(string)
	version := dbRecord[versionKey].(int64)
	task.Version = uint64(version)
	return true
}

func (i *dbAggregateVersionIterator) Err() error {
	return i.iter.Err()
}

// Query retrieves the latest snapshot version per aggregate for thw number of aggregates specified by 'limit'
func (s *EventStore) Query(ctx context.Context, limit int, taskHandler maintenance.TaskHandler) error {
	opts := options.FindOptions{}
	opts.SetLimit(int64(limit))
	iter, err := s.client.Database(s.DBName()).Collection(maintenanceCName).Find(ctx, bson.M{}, &opts)
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
		return fmt.Errorf("could not remove record with aggregate ID %s and/or version %d", task.AggregateID, task.Version)
	}

	return nil
}
