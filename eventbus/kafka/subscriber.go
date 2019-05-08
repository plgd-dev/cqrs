package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-ocf/cqrs/event"
	"github.com/go-ocf/cqrs/eventbus"
	protoEventBus "github.com/go-ocf/cqrs/protobuf/eventbus"

	sarama "github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
)

// Subscriber implements a eventbus.Subscriber interface.
type Subscriber struct {
	brokers         []string
	config          *sarama.Config
	dataUnmarshaler event.UnmarshalerFunc
	errFunc         eventbus.ErrFunc
	goroutinePoolGo eventbus.GoroutinePoolGoFunc
}

//Observer handles events from kafka
type Observer struct {
	subscriptionId  string
	topics          []string
	client          *cluster.Client
	consumer        *cluster.Consumer
	ctx             context.Context
	cancel          context.CancelFunc
	dataUnmarshaler event.UnmarshalerFunc
	eventHandler    event.Handler
	wg              sync.WaitGroup
	errFunc         eventbus.ErrFunc
	lock            sync.Mutex
}

// NewSubscriber creates a subscriber.
func NewSubscriber(brokers []string, config *sarama.Config, eventUnmarshaler event.UnmarshalerFunc, goroutinePoolGo eventbus.GoroutinePoolGoFunc, errFunc eventbus.ErrFunc) (*Subscriber, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("invalid brokers")
	}
	if config == nil {
		return nil, fmt.Errorf("invalid config")
	}
	if eventUnmarshaler == nil {
		return nil, fmt.Errorf("invalid eventUnmarshaler")
	}
	if errFunc == nil {
		return nil, fmt.Errorf("invalid errFunc")
	}

	return &Subscriber{
		brokers:         brokers,
		config:          config,
		dataUnmarshaler: eventUnmarshaler,
		errFunc:         errFunc,
		goroutinePoolGo: goroutinePoolGo,
	}, nil
}

// Subscribe creates a observer that listen on events from topics.
func (b *Subscriber) Subscribe(ctx context.Context, subscriptionID string, topics []string, eh event.Handler) (eventbus.Observer, error) {
	observer, err := b.newObservation(ctx, subscriptionID, eventbus.NewGoroutinePoolHandler(b.goroutinePoolGo, eh, b.errFunc))
	if err != nil {
		return nil, fmt.Errorf("cannot observe: %v", err)
	}

	err = observer.SetTopics(ctx, topics)
	if err != nil {
		return nil, fmt.Errorf("cannot run observation: %v", err)
	}
	return observer, nil
}

func (b *Subscriber) newObservation(ctx context.Context, subscriptionId string, eh event.Handler) (*Observer, error) {
	config := cluster.NewConfig()
	config.Config = *b.config
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = false
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	client, err := cluster.NewClient(b.brokers, config)
	if err != nil {
		return nil, fmt.Errorf("cannot connect client for subscription: %v", err)
	}

	return &Observer{
		client:          client,
		subscriptionId:  subscriptionId,
		dataUnmarshaler: b.dataUnmarshaler,
		eventHandler:    eh,
		errFunc:         b.errFunc,
	}, nil
}

func (o *Observer) cleanUp() {
	if o.cancel != nil {
		o.cancel()
	}
	o.wg.Wait()
	if o.consumer != nil {
		o.consumer.Close()
	}
	o.consumer = nil
}

func (o *Observer) SetTopics(ctx context.Context, topics []string) error {
	o.lock.Lock()
	defer o.lock.Unlock()
	o.cleanUp()
	o.topics = topics

	//observer just stop watching topics
	if len(topics) == 0 {
		return nil
	}

	consumer, err := cluster.NewConsumerFromClient(o.client, o.subscriptionId, topics)
	if err != nil {
		return fmt.Errorf("cannot create consumer for subscription: %v", err)
	}
	obsCtx, obsCancel := context.WithCancel(ctx)
	o.topics = topics
	o.consumer = consumer
	o.ctx = obsCtx
	o.cancel = obsCancel
	return o.run(ctx)
}

// Close cancel observation and close connection to kafka.
func (o *Observer) Close() error {
	o.lock.Lock()
	o.cleanUp()
	o.lock.Unlock()
	return o.client.Close()
}

func (o *Observer) run(ctx context.Context) error {
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		o.handle()
	}()
	return nil
}

func (o *Observer) handle() {
	for {
		select {
		case msg, ok := <-o.consumer.Messages():
			if ok {
				err := o.handleMessage(msg)
				if err != nil {
					o.errFunc(err)
				}
			}
		case err := <-o.consumer.Errors():
			o.errFunc(fmt.Errorf("could not receive: %v", err))
		case <-o.ctx.Done():
			o.cancel()
			return
		}
	}

}

type iter struct {
	e               protoEventBus.Event
	dataUnmarshaler func(v interface{}) error
	hasNext         bool
}

func (i *iter) Next(ctx context.Context, e *event.EventUnmarshaler) bool {
	if !i.hasNext {
		return false
	}
	e.Version = i.e.Version
	e.AggregateId = i.e.AggregateId
	e.GroupId = i.e.GroupId
	e.EventType = i.e.EventType
	e.Unmarshal = i.dataUnmarshaler
	i.hasNext = false
	return true
}

func (i *iter) Err() error {
	return nil
}

func (o *Observer) handleMessage(msg *sarama.ConsumerMessage) error {

	var e protoEventBus.Event

	err := e.Unmarshal(msg.Value)
	if err != nil {
		return fmt.Errorf("could not unmarshal event: %v", err)
	}

	i := iter{
		hasNext: true,
		e:       e,
		dataUnmarshaler: func(v interface{}) error {
			return o.dataUnmarshaler(e.Data, v)
		},
	}

	if err := o.eventHandler.Handle(o.ctx, &i); err != nil {
		return fmt.Errorf("could not handle event: %v", err)
	}
	o.consumer.MarkOffset(msg, "")
	return nil
}
