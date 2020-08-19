package mongodb

import (
	"context"
	"os"
	"testing"

	"github.com/plgd-dev/cqrs/eventstore/test"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestEventStore(t *testing.T) {
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
		"events", 1,
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

	t.Log("event store with default namespace")
	test.AcceptanceTest(t, ctx, store)
}
