package mongodb

import (
	"context"
	"os"
	"testing"

	"github.com/panjf2000/ants"

	"github.com/globalsign/mgo/bson"
	"github.com/go-ocf/cqrs/eventstore/test"
	"github.com/stretchr/testify/assert"
)

func TestEventStore(t *testing.T) {
	// Local Mongo testing with Docker
	url := os.Getenv("MONGO_HOST")

	if url == "" {
		// Default to localhost
		url = "localhost:27017"
	}

	pool, err := ants.NewPool(16)
	assert.NoError(t, err)
	defer pool.Release()

	store, err := NewEventStore(url, "test_mongodb", "events", 1, pool, bson.Marshal, bson.Unmarshal, nil)
	assert.NoError(t, err)
	assert.NotNil(t, store)

	defer store.Close()
	defer func() {
		t.Log("clearing db")
		err := store.Clear(context.Background())
		assert.NoError(t, err)
	}()

	t.Log("event store with default namespace")
	test.AcceptanceTest(t, context.Background(), store)
}
