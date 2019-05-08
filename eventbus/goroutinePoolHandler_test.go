package eventbus

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/go-ocf/cqrs/event"
)

type mockEventHandler struct {
	events    []event.EventUnmarshaler
	processed chan struct{}
}

func newMockEventHandler() *mockEventHandler {
	return &mockEventHandler{events: make([]event.EventUnmarshaler, 0, 10), processed: make(chan struct{})}
}

func (eh *mockEventHandler) Handle(ctx context.Context, iter event.Iter) error {
	var eu event.EventUnmarshaler

	for iter.Next(ctx, &eu) {
		if eu.EventType == "" {
			return errors.New("cannot determine type of event")
		}
		eh.events = append(eh.events, eu)
	}
	close(eh.processed)

	return iter.Err()
}

func TestGoroutinePoolHandler_Handle(t *testing.T) {
	type fields struct {
		goroutinePoolGo GoroutinePoolGoFunc
	}
	type args struct {
		ctx  context.Context
		iter event.Iter
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantErr     bool
		wantTimeout bool
		want        []event.EventUnmarshaler
	}{
		{
			name: "empty",
			fields: fields{
				goroutinePoolGo: func(f func()) error { go f(); return nil },
			},
			args: args{
				ctx:  context.Background(),
				iter: &iter{},
			},
			wantTimeout: true,
		},
		{
			name: "valid",
			fields: fields{
				goroutinePoolGo: func(f func()) error { go f(); return nil },
			},
			args: args{
				ctx: context.Background(),
				iter: &iter{
					events: []event.EventUnmarshaler{
						event.EventUnmarshaler{
							Version:   0,
							EventType: "type-0",
						},
						event.EventUnmarshaler{
							Version:   1,
							EventType: "type-1",
						},
						event.EventUnmarshaler{
							Version:   2,
							EventType: "type-2",
						},
					},
				},
			},
			want: []event.EventUnmarshaler{
				event.EventUnmarshaler{
					Version:   0,
					EventType: "type-0",
				},
				event.EventUnmarshaler{
					Version:   1,
					EventType: "type-1",
				},
				event.EventUnmarshaler{
					Version:   2,
					EventType: "type-2",
				},
			},
		},
		{
			name: "error",
			fields: fields{
				goroutinePoolGo: func(f func()) error { go f(); return nil },
			},
			args: args{
				ctx: context.Background(),
				iter: &iter{
					events: []event.EventUnmarshaler{
						event.EventUnmarshaler{
							Version:   0,
							EventType: "type-0",
						},
						event.EventUnmarshaler{
							Version: 1,
						},
					},
				},
			},
			want: []event.EventUnmarshaler{
				event.EventUnmarshaler{
					Version:   0,
					EventType: "type-0",
				},
			},
			wantErr:     true,
			wantTimeout: true,
		},
		{
			name: "valid without goroutinePoolGo",
			args: args{
				ctx: context.Background(),
				iter: &iter{
					events: []event.EventUnmarshaler{
						event.EventUnmarshaler{
							Version:   0,
							EventType: "type-0",
						},
						event.EventUnmarshaler{
							Version:   1,
							EventType: "type-1",
						},
						event.EventUnmarshaler{
							Version:   2,
							EventType: "type-2",
						},
					},
				},
			},
			want: []event.EventUnmarshaler{
				event.EventUnmarshaler{
					Version:   0,
					EventType: "type-0",
				},
				event.EventUnmarshaler{
					Version:   1,
					EventType: "type-1",
				},
				event.EventUnmarshaler{
					Version:   2,
					EventType: "type-2",
				},
			},
		},
		{
			name: "error without goroutinePoolGo",
			fields: fields{
				goroutinePoolGo: func(f func()) error { go f(); return nil },
			},
			args: args{
				ctx: context.Background(),
				iter: &iter{
					events: []event.EventUnmarshaler{
						event.EventUnmarshaler{
							Version:   0,
							EventType: "type-0",
						},
						event.EventUnmarshaler{
							Version: 1,
						},
					},
				},
			},
			want: []event.EventUnmarshaler{
				event.EventUnmarshaler{
					Version:   0,
					EventType: "type-0",
				},
			},
			wantErr:     true,
			wantTimeout: true,
		},
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eh := newMockEventHandler()
			ep := NewGoroutinePoolHandler(
				tt.fields.goroutinePoolGo,
				eh,
				func(err error) {
					if tt.wantErr {
						assert.Error(t, err)
					} else {
						assert.NoError(t, err)
					}
				})
			err := ep.Handle(tt.args.ctx, tt.args.iter)
			assert.NoError(t, err)
			select {
			case <-eh.processed:
				assert.Equal(t, tt.want, eh.events)
			case <-time.After(time.Millisecond * 100):
				if !tt.wantTimeout {
					assert.NoError(t, fmt.Errorf("timeout"))
				}
			}
		})
	}
}
