package google_pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pubsubV0 "cloud.google.com/go/pubsub"
	"github.com/go-playground/validator/v10"
)

type (
	SubscriptionConfig struct {
		Topics                    []string      `validate:"required,min=1"`
		SubscriptionID            string        `validate:"required"`
		AckDeadline               time.Duration `validate:"required,min=1s,max=600s"`
		EnableExactlyOnceDelivery bool
		//EnableMessageOrdering must be combined with orderingKey on message. Otherwise, it will fail to publish.
		EnableMessageOrdering bool
		EarlyAck              bool
	}

	SubscriptionHandlerFunc func(ctx context.Context, message *Message) error
	SubscriptionOnErrorFunc func(ctx context.Context, err error, header Header)
	Subscriber              struct {
		Config  SubscriptionConfig
		Handler SubscriptionHandlerFunc
		OnError SubscriptionOnErrorFunc
	}

	Subscribers  []Subscriber
	Subscription struct {
		projectID   string
		client      *pubsubV0.Client
		subscribers Subscribers
	}
)

func (s *Subscription) Register(ctx context.Context, subscribers ...*Subscriber) error {
	attachedChannel := make(chan Subscriber, len(subscribers))
	var wg sync.WaitGroup
	wg.Add(len(subscribers))
	validate := validator.New()

	for _, subscriber := range subscribers {
		if err := validate.StructCtx(ctx, subscriber.Config); err != nil {
			return fmt.Errorf("%v: %w", err, ErrValidation)
		}

		go func(sub *Subscriber) {
			if len(subscribers) > len(s.subscribers) {
				attachedChannel <- *sub
				wg.Done()
			}
			subscriptions, err := s.getOrCreateSubscription(ctx, &sub.Config)
			if err != nil {
				sub.OnError(ctx, err, map[string]string{"location": fmt.Sprintf("registration subscription: %s from topic %v", sub.Config.SubscriptionID, sub.Config.Topics)})
				return
			}

			for index, subscription := range subscriptions {
				err := subscription.Receive(ctx, func(ctx context.Context, message *pubsubV0.Message) {
					defer handlePanic(ctx, sub, message)
					if sub.Config.EarlyAck {
						message.Ack()
					}

					err := sub.Handler(ctx, &Message{
						Topic:       sub.Config.Topics[index],
						Content:     Content{data: message.Data},
						OriginalKey: message.OrderingKey,
						Headers:     message.Attributes,
					})
					if err != nil {
						sub.OnError(ctx, err, message.Attributes)
						var pubSubError PubsubError
						if errors.As(err, &pubSubError) {
							if pubSubError.Retriable() {
								return
							}
						}
					}
					message.Ack()
				})
				if err != nil {
					sub.OnError(ctx, err, nil)
					_ = s.client.Close()
				}
			}
		}(subscriber)
	}
	wg.Wait()
	close(attachedChannel)
	for item := range attachedChannel {
		s.subscribers = append(s.subscribers, item)
	}
	return nil
}

func (s *Subscription) getOrCreateSubscription(ctx context.Context, config *SubscriptionConfig) ([]*pubsubV0.Subscription, error) {
	subs := make([]*pubsubV0.Subscription, 0, len(config.Topics))
	for index := range config.Topics {
		subscriptionID := fmt.Sprintf("%s-%d", config.SubscriptionID, index)
		exists, _ := s.client.
			SubscriptionInProject(subscriptionID, s.projectID).
			Exists(ctx)
		if !exists {

			if exists, _ := s.client.TopicInProject(config.Topics[index], s.projectID).Exists(ctx); !exists {
				if _, err := s.client.CreateTopic(ctx, config.Topics[index]); err != nil {
					return nil, err
				}
			}

			sub, err := s.client.CreateSubscription(ctx, subscriptionID,
				pubsubV0.SubscriptionConfig{
					AckDeadline:               config.AckDeadline,
					Topic:                     s.client.TopicInProject(config.Topics[index], s.projectID),
					EnableExactlyOnceDelivery: config.EnableExactlyOnceDelivery,
					EnableMessageOrdering:     config.EnableMessageOrdering,
				},
			)
			if err != nil {
				return nil, err
			}
			subs = append(subs, sub)
			continue
		}
		subs = append(subs, s.client.SubscriptionInProject(subscriptionID, s.projectID))
	}

	return subs, nil
}

func (s *Subscription) Close(_ context.Context) error {
	s.subscribers = nil
	return s.client.Close()
}

func handlePanic(ctx context.Context, sub *Subscriber, message *pubsubV0.Message) {
	if r := recover(); r != nil {
		if recoveryError, ok := r.(error); ok {
			sub.OnError(ctx, recoveryError, message.Attributes)
			return
		}
		if recoveryError, ok := r.(string); ok {
			sub.OnError(ctx, errors.New(recoveryError), message.Attributes)
			return
		}
	}
}
