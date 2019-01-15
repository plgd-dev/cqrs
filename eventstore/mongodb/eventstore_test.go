package mongodb

import (
	"context"
	"os"
	"testing"

	"github.com/globalsign/mgo/bson"
	"github.com/go-ocf/cqrs/eventstore"
	"github.com/stretchr/testify/assert"
)

func TestEventStore(t *testing.T) {
	// Local Mongo testing with Docker
	url := os.Getenv("MONGO_HOST")

	if url == "" {
		// Default to localhost
		url = "localhost:27017"
	}

	store, err := NewEventStore(url, "test_mongodb", "events", bson.Marshal, bson.Unmarshal)
	assert.NoError(t, err)
	assert.NotNil(t, store)

	defer store.Close()
	defer func() {
		t.Log("clearing db")
		err := store.Clear(context.Background())
		assert.NoError(t, err)
	}()

	t.Log("event store with default namespace")
	eventstore.AcceptanceTest(t, context.Background(), store)
}
