package eventbus

import (
	"context"
	"fmt"
	"sync"

	"github.com/plgd-dev/cqrs/event"
)

// ErrFunc used by handler to report error from observation
type ErrFunc func(err error)

// GoroutinePoolGoFunc processes actions via provided function
type GoroutinePoolGoFunc func(func()) error

// GoroutinePoolHandler submit events to goroutine pool for process them.
type GoroutinePoolHandler struct {
	lock                     sync.Mutex
	aggregateEventsContainer map[string]*eventsProcessor

	goroutinePoolGo GoroutinePoolGoFunc
	eventsHandler   event.Handler
	errFunc         ErrFunc
}

// NewGoroutinePoolHandler creates new event processor.
func NewGoroutinePoolHandler(
	goroutinePoolGo GoroutinePoolGoFunc,
	eventsHandler event.Handler,
	errFunc ErrFunc) *GoroutinePoolHandler {

	return &GoroutinePoolHandler{
		goroutinePoolGo:          goroutinePoolGo,
		eventsHandler:            eventsHandler,
		errFunc:                  errFunc,
		aggregateEventsContainer: make(map[string]*eventsProcessor),
	}
}

func (ep *GoroutinePoolHandler) run(ctx context.Context, p *eventsProcessor) error {
	if ep.goroutinePoolGo == nil {
		err := p.process(ctx, ep.eventsHandler)
		ep.tryToDelete(p.name)
		return err
	}
	err := ep.goroutinePoolGo(func() {
		err := p.process(ctx, ep.eventsHandler)
		if err != nil {
			ep.errFunc(err)
		}
		ep.tryToDelete(p.name)
	})
	if err != nil {
		return fmt.Errorf("cannot execute goroutine pool go function: %w", err)
	}
	return nil
}

// Handle pushes event to queue and process the queue by goroutine pool.
func (ep *GoroutinePoolHandler) Handle(ctx context.Context, iter event.Iter) (err error) {
	var eu event.EventUnmarshaler
	lastId := ""
	events := make([]event.EventUnmarshaler, 0, 128)
	for iter.Next(ctx, &eu) {
		id := eventToName(eu)
		if lastId != "" && id != lastId || len(events) >= 128 {
			ed := ep.getEventsData(id)
			spawnGo := ed.push(events)
			if spawnGo {
				err := ep.run(ctx, ed)
				if err != nil {
					return fmt.Errorf("cannot handle events: %w", err)
				}
			}
			events = make([]event.EventUnmarshaler, 0, 128)
		}
		lastId = id
		events = append(events, eu)
	}
	if len(events) > 0 {
		ed := ep.getEventsData(eventToName(events[0]))
		spawnGo := ed.push(events)
		if spawnGo {
			err := ep.run(ctx, ed)
			if err != nil {
				return fmt.Errorf("cannot handle events: %w", err)
			}
		}
	}
	return nil
}

func (ep *GoroutinePoolHandler) getEventsData(name string) *eventsProcessor {
	ep.lock.Lock()
	defer ep.lock.Unlock()
	ed, ok := ep.aggregateEventsContainer[name]
	if !ok {
		ed = newEventsProcessor(name)
		ep.aggregateEventsContainer[name] = ed
	}
	return ed
}

func (ep *GoroutinePoolHandler) tryToDelete(name string) {
	ep.lock.Lock()
	defer ep.lock.Unlock()
	ed, ok := ep.aggregateEventsContainer[name]
	if ok {
		ed.lock.Lock()
		defer ed.lock.Unlock()
		if !ed.isProcessed {
			delete(ep.aggregateEventsContainer, name)
		}
	}
}

type eventsProcessor struct {
	name        string
	queue       []event.EventUnmarshaler
	isProcessed bool
	lock        sync.Mutex
}

func newEventsProcessor(name string) *eventsProcessor {
	return &eventsProcessor{
		name:  name,
		queue: make([]event.EventUnmarshaler, 0, 128),
	}
}

func (ed *eventsProcessor) push(events []event.EventUnmarshaler) bool {
	ed.lock.Lock()
	defer ed.lock.Unlock()
	ed.queue = append(ed.queue, events...)
	if ed.isProcessed == false {
		ed.isProcessed = true
		return true
	}
	return false
}

func (ed *eventsProcessor) pop() []event.EventUnmarshaler {
	ed.lock.Lock()
	defer ed.lock.Unlock()
	if len(ed.queue) > 0 {
		res := ed.queue
		ed.queue = make([]event.EventUnmarshaler, 0, 16)
		return res
	}
	ed.isProcessed = false
	return nil
}

func (ed *eventsProcessor) process(ctx context.Context, eh event.Handler) error {
	for {
		events := ed.pop()
		if len(events) == 0 {
			return nil
		}

		i := iter{
			events: events,
		}

		if err := eh.Handle(ctx, &i); err != nil {
			ed.lock.Lock()
			defer ed.lock.Unlock()
			ed.isProcessed = false
			return fmt.Errorf("cannot process event: %w", err)
		}
	}
}

func eventToName(ev event.EventUnmarshaler) string {
	return ev.GroupId + "." + ev.AggregateId
}

type iter struct {
	events []event.EventUnmarshaler
	idx    int
}

func (i *iter) Next(ctx context.Context, e *event.EventUnmarshaler) bool {
	if i.idx >= len(i.events) {
		return false
	}
	*e = i.events[i.idx]
	i.idx++
	return true
}

func (i *iter) Err() error {
	return nil
}
