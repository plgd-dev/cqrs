package kafka

import (
	"context"
	"errors"
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
}

//Observer handles events from kafka
type Observer struct {
	consumer        *cluster.Consumer
	ctx             context.Context
	cancel          context.CancelFunc
	errCh           chan error
	dataUnmarshaler event.UnmarshalerFunc
	eventHandler    event.EventHandler
	wg              sync.WaitGroup
}

// NewSubscriber creates a subscriber.
func NewSubscriber(brokers []string, config *sarama.Config, eventUnmarshaler event.UnmarshalerFunc) *Subscriber {
	return &Subscriber{
		brokers:         brokers,
		config:          config,
		dataUnmarshaler: eventUnmarshaler,
	}
}

// Subscribe creates a observer that listen on events from topics.
func (b *Subscriber) Subscribe(ctx context.Context, subscriptionID string, topics []string, eh event.EventHandler) (eventbus.Observer, error) {
	observer, err := b.newObservation(ctx, subscriptionID, topics, eh)
	if err != nil {
		return nil, fmt.Errorf("cannot observe: %v", err)
	}

	err = observer.run(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot run observation: %v", err)
	}
	return observer, nil
}

func (b *Subscriber) newObservation(ctx context.Context, subscriptionID string, topics []string, eh event.EventHandler) (*Observer, error) {
	config := cluster.NewConfig()
	config.Config = *b.config
	//config.Config.ClientID = subscriptionID
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true

	consumer, err := cluster.NewConsumer(b.brokers, subscriptionID, topics, config)
	if err != nil {
		return nil, fmt.Errorf("cannot create consumer for subscription: %v", err)
	}
	obsCtx, obsCancel := context.WithCancel(ctx)
	return &Observer{
		consumer:        consumer,
		ctx:             obsCtx,
		cancel:          obsCancel,
		errCh:           make(chan error),
		dataUnmarshaler: b.dataUnmarshaler,
		eventHandler:    eh,
	}, nil
}

// Errors returns an error channel where async handling errors are sent.
func (o *Observer) Errors() <-chan error {
	return o.errCh
}

// Cancel cancel observation and close connection to kafka.
func (o *Observer) Cancel() error {
	o.cancel()
	o.wg.Wait()
	close(o.errCh)
	return o.consumer.Close()
}

func (o *Observer) run(ctx context.Context) error {
	sync := make(chan interface{})
	o.wg.Add(1)
	go func(sync chan interface{}) {
		defer o.wg.Done()
		o.handle(func() {
			if sync != nil {
				close(sync)
				sync = nil
			}
		})
	}(sync)
	select {
	case <-sync:
	case <-ctx.Done():
		o.Cancel()
		return errors.New("unexpected end of initialization of observation")
	}
	return nil
}

func (o *Observer) handle(rebalanceOk func()) {
	for {
		select {
		case msg, ok := <-o.consumer.Messages():
			if ok {
				err := o.handleMessage(msg)
				if err != nil {
					o.error(err)
				}
			}
		case ntf := <-o.consumer.Notifications():
			if ntf.Type == cluster.RebalanceOK {
				rebalanceOk()
			}
		case err := <-o.consumer.Errors():
			o.error(fmt.Errorf("could not receive: %v", err))
		case <-o.ctx.Done():
			o.cancel()
			return
		}
	}

}

func (o *Observer) error(err error) {
	select {
	case o.errCh <- err:
	default:
	}
	return
}

func (o *Observer) handleMessage(msg *sarama.ConsumerMessage) error {

	var e protoEventBus.Event

	err := e.Unmarshal(msg.Value)
	if err != nil {
		return fmt.Errorf("could not unmarshal event: %v", err)
	}

	eventUnmarshaler := eventbus.EventUnmarshaler{
		Event:           e,
		DataUnmarshaler: o.dataUnmarshaler,
	}

	if err := o.eventHandler.HandleEvent(o.ctx, *e.Path, eventUnmarshaler); err != nil {
		return fmt.Errorf("could not handle event: %v", err)
	}
	o.consumer.MarkOffset(msg, "")
	return nil
}
