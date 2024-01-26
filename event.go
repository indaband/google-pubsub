package google_pubsub

import (
	"context"
	"errors"
	"time"

	"github.com/cenkalti/backoff"
)

type (
	LocalSubscriptionConfig struct {
		Topic           string        `validate:"required"`
		SubscriptionID  string        `validate:"required"`
		TickerTime      time.Duration `validate:"min=1s, max=10s"`
		PullingQuantity int32         `validate:"gte=1,lte=1000"`
		EarlyAck        bool
	}
	LocalSubscriber struct {
		Config  LocalSubscriptionConfig
		Handler func(ctx context.Context, message *Message) error
		OnError func(ctx context.Context, err error, header Header)
	}
)

type Event struct {
	MaxRetries    uint64
	subscriptions []LocalSubscriber
	Channel       chan Message
}

// NewEvent allow client to retrieve in Event instance
func NewEvent(maxRetries uint64) Event {
	return Event{
		MaxRetries:    maxRetries,
		subscriptions: make([]LocalSubscriber, 0, 10),
		Channel:       make(chan Message, maxRetries),
	}
}

// Subscribe allow client to subscribe
func (e *Event) Subscribe(listeners ...LocalSubscriber) {
	e.subscriptions = append(e.subscriptions, listeners...)
}

// Dispatch event based on event name
func (e *Event) Send(ctx context.Context, message *Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	select {
	case e.Channel <- *message:
		return nil
	case <-ctx.Done():
		return errors.New("there is not listeners available. Channel is busy")
	}
}

// Start should be called once on start up
func (e *Event) Start(ctx context.Context) error {
	const numWorkers = 4
	for w := 1; w <= numWorkers; w++ {
		go e.worker(ctx)
	}
	return nil
}

// Close allow client to stop channels
func (e *Event) Close() {
	close(e.Channel)
}

func (e *Event) worker(ctx context.Context) {
	for message := range e.Channel {
		for _, subscription := range e.subscriptions {
			if message.Topic == subscription.Config.Topic {
				go func(maxRetries uint64, sub LocalSubscriber, message Message) {
					_ = retry(maxRetries, func() error {
						return sub.Handler(ctx, &message)
					}, ctx, sub, &message)
				}(e.MaxRetries, subscription, message)
			}
		}
	}
}

func retry(maxRetries uint64, fn func() error, ctx context.Context, sub LocalSubscriber, message *Message) error {
	if err := backoff.Retry(fn, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), maxRetries)); err != nil {
		sub.OnError(ctx, err, message.Headers)
		return err
	}
	return nil
}
