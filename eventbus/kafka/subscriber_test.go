package kafka

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/go-ocf/cqrs/eventbus/test"
	"github.com/gofrs/uuid"
	"github.com/stretchr/testify/assert"
)

func TestSubscriber(t *testing.T) {
	// Connect to localhost if not running inside docker
	broker := os.Getenv("KAFKA_EMULATOR_BOOTSTRAP_SERVER")
	if broker == "" {
		broker = "localhost:9092"
	}

	topics := []string{"test_kafka_topic0_" + uuid.Must(uuid.NewV4()).String(), "test_kafka_topic1" + uuid.Must(uuid.NewV4()).String()}

	config := sarama.NewConfig()
	config.Producer.Flush.MaxMessages = 1

	timeout := time.Second * 30
	waitForSubscription := time.Second * 7

	publisher, err := NewPublisher(
		[]string{broker},
		config,
		json.Marshal)
	assert.NoError(t, err)
	assert.NotNil(t, publisher)

	subscriber, err := NewSubscriber(
		[]string{broker},
		config,
		json.Unmarshal,
		func(err error) { assert.NoError(t, err) },
	)
	assert.NotNil(t, subscriber)

	test.AcceptanceTest(t, context.Background(), timeout, waitForSubscription, topics, publisher, subscriber)
}
