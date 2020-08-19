package mongodb

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/plgd-dev/cqrs/eventstore/maintenance"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

type mockRecordHandler struct {
	lock  sync.Mutex
	tasks map[string]maintenance.Task
}

func newMockRecordHandler() *mockRecordHandler {
	return &mockRecordHandler{tasks: make(map[string]maintenance.Task)}
}

func (eh *mockRecordHandler) SetElement(aggregateID string, task maintenance.Task) {
	var aggregate maintenance.Task
	var ok bool

	eh.lock.Lock()
	defer eh.lock.Unlock()
	if aggregate, ok = eh.tasks[aggregateID]; !ok {
		eh.tasks[aggregateID] = maintenance.Task{AggregateID: task.AggregateID, Version: task.Version}
	}
	aggregate.AggregateID = task.AggregateID
	aggregate.Version = task.Version
}

func (eh *mockRecordHandler) Handle(ctx context.Context, iter maintenance.Iter) error {
	var task maintenance.Task

	for iter.Next(ctx, &task) {
		eh.SetElement(task.AggregateID, task)
	}
	return nil
}

func TestMaintenance(t *testing.T) {
	// Local Mongo testing with Docker
	host := os.Getenv("MONGO_HOST")

	if host == "" {
		// Default to localhost
		host = "localhost:27017"
	}
	ctx := context.Background()

	store, err := NewEventStore(
		ctx,
		host,
		"test_mongodb",
		"maintenance", 1,
		func(f func()) error { go f(); return nil },
		bson.Marshal,
		bson.Unmarshal,
		nil)
	require.NoError(t, err)
	require.NotNil(t, store)

	defer store.Close(ctx)
	defer func() {
		t.Log("clearing db")
		err := store.Clear(ctx)
		require.NoError(t, err)
	}()

	aggregateID1 := "aggregateID1"
	tasksToSave := []maintenance.Task{
		maintenance.Task{
			AggregateID: aggregateID1,
		},
		maintenance.Task{
			AggregateID: aggregateID1,
			Version:     1,
		},
		maintenance.Task{
			AggregateID: aggregateID1,
			Version:     2,
		},
		maintenance.Task{
			AggregateID: aggregateID1,
			Version:     3,
		},
		maintenance.Task{
			AggregateID: aggregateID1,
			Version:     4,
		},
	}

	t.Log("insert maintenance record without body")
	err = store.Insert(ctx, maintenance.Task{})
	require.Error(t, err)

	t.Log("insert maintenance record")
	err = store.Insert(ctx, tasksToSave[1])
	require.NoError(t, err)

	t.Log("insert maintenance record with higher version")
	err = store.Insert(ctx, tasksToSave[4])
	require.NoError(t, err)

	t.Log("query maintenance records")
	eh1 := newMockRecordHandler()
	err = store.Query(ctx, 777, eh1)
	require.NoError(t, err)
	require.Equal(t, tasksToSave[4], eh1.tasks[aggregateID1])

	t.Log("insert maintenance record with lower version")
	err = store.Insert(ctx, tasksToSave[3])
	require.Error(t, err)

	t.Log("query maintenance records")
	eh2 := newMockRecordHandler()
	err = store.Query(ctx, 777, eh2)
	require.NoError(t, err)
	require.Equal(t, tasksToSave[4], eh2.tasks[aggregateID1])

	t.Log("remove maintenance record - incorrect version")
	err = store.Remove(ctx, tasksToSave[3])
	require.Error(t, err)

	t.Log("remove maintenance record")
	err = store.Remove(ctx, tasksToSave[4])
	require.NoError(t, err)

	t.Log("query maintenance records - empty collection")
	eh3 := newMockRecordHandler()
	err = store.Query(ctx, 777, eh3)
	require.NoError(t, err)
	require.Equal(t, 0, len(eh3.tasks))
}
