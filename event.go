package google_pubsub

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff"
)

type Context struct {
	Metadata  map[string]string
	EventName string
	Data      []byte
}

type Notifier interface {
	Dispatch(name string, data []byte, metadata map[string]string) error
	Close()
}

type Listener interface {
	EventName() string
	GroupID() string
	Caller([]byte, map[string]string) error
	OnSuccess(metadata map[string]string)
	OnError(err error, metadata map[string]string)
}

type Event struct {
	MaxRetries uint64
	listeners  []Listener
	Channel    chan Context
}

// NewEvent allow client to retrieve in Event instance
func NewEvent(maxRetries uint64) Event {
	return Event{
		MaxRetries: maxRetries,
		listeners:  make([]Listener, 0, 10),
		Channel:    make(chan Context, maxRetries),
	}
}

// Subscribe allow client to subscribe
func (e *Event) Subscribe(listeners ...Listener) {
	e.listeners = append(e.listeners, listeners...)
}

// Dispatch event based on event name
func (e *Event) Dispatch(name string, data []byte, metadata map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	select {
	case e.Channel <- Context{EventName: name, Data: data, Metadata: metadata}:
		return nil
	case <-ctx.Done():
		return errors.New("there is not listeners available. Channel is busy")
	}
}

// Start should be called once on start up
func (e *Event) Start() error {
	const numWorkers = 4
	for w := 1; w <= numWorkers; w++ {
		go e.worker()
	}
	return nil
}

// Close allow client to stop channels
func (e *Event) Close() {
	close(e.Channel)
}

func (e *Event) worker() {
	for event := range e.Channel {
		for _, listener := range e.listeners {
			if event.EventName == listener.EventName() {
				go func(maxRetries uint64, l Listener, metadata map[string]string, data []byte) {
					_ = retry(maxRetries, func() error {
						return l.Caller(data, metadata)
					}, l, metadata)
				}(e.MaxRetries, listener, event.Metadata, event.Data)
			}
		}
	}
}

func retry(maxRetries uint64, fn func() error, listener Listener, metadata map[string]string) error {
	if err := backoff.Retry(fn, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), maxRetries)); err != nil {
		listener.OnError(err, metadata)
		return err
	}
	listener.OnSuccess(metadata)
	return nil
}
