package google_pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	pubsubV0 "cloud.google.com/go/pubsub"
	providerpubsub "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"github.com/go-playground/validator/v10"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	SubscriptionConfig struct {
		Topic           string        `validate:"required"`
		SubscriptionID  string        `validate:"required"`
		TickerTime      time.Duration `validate:"min=1s, max=10s"`
		PullingQuantity int32         `validate:"gte=1,lte=1000"`
		EarlyAck        bool
	}

	Subscriber struct {
		Config       SubscriptionConfig
		Handler      func(ctx context.Context, message *Message) error
		OnError      func(ctx context.Context, err error, header Header)
		subscription *pubsubpb.Subscription
	}

	Subscribers  []Subscriber
	Subscription struct {
		projectID   string
		client      *providerpubsub.SubscriberClient
		subscribers Subscribers
	}
)

func (s *Subscription) Register(ctx context.Context, subscribers ...Subscriber) error {
	attachedChannel := make(chan Subscriber, len(subscribers))
	var wg sync.WaitGroup
	wg.Add(len(subscribers))

	validate := validator.New()

	for _, sub := range subscribers {
		if err := validate.StructCtx(ctx, sub.Config); err != nil {
			return fmt.Errorf("validation error: %w", err)
		}

		go func(subscriber Subscriber) {
			ticker := time.NewTicker(subscriber.Config.TickerTime)
			defer ticker.Stop()

			for {
				select {
				case <-ticker.C:
					if err := s.getOrCreateSubscription(ctx, &subscriber); err != nil {
						if status.Convert(err).Code() != codes.Canceled && status.Convert(err).Code() != codes.AlreadyExists {
							subscriber.OnError(ctx, err, nil)
							continue
						}
					}
					attachedChannel <- subscriber
					wg.Done()
					s.pullMessage(ctx, &subscriber)
				case <-ctx.Done():
					ticker.Stop()
					_ = s.client.Close()
				}
			}
		}(sub)
	}

	wg.Wait()
	close(attachedChannel)
	for item := range attachedChannel {
		s.subscribers = append(s.subscribers, item)
	}
	return nil
}

func (s *Subscription) getOrCreateSubscription(ctx context.Context, sub *Subscriber) error {
	if subscription, err := s.client.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{
		Subscription: fmt.Sprintf("projects/%s/subscriptions/%s", s.projectID, sub.Config.SubscriptionID),
	}); err == nil {
		sub.subscription = subscription
		return nil
	}
	subscription, err := s.client.CreateSubscription(
		ctx, &pubsubpb.Subscription{
			Name:  fmt.Sprintf("projects/%s/subscriptions/%s", s.projectID, sub.Config.SubscriptionID),
			Topic: fmt.Sprintf("projects/%s/topics/%s", s.projectID, sub.Config.Topic),
		})

	if err != nil {
		if status.Convert(err).Code() == codes.NotFound {
			cli, err := pubsubV0.
				NewClient(ctx, s.projectID)
			if err != nil {
				return fmt.Errorf("fail to create automatic topic %q: %w", sub.Config.Topic, err)
			}

			defer func() { _ = cli.Close() }()

			if _, err := cli.CreateTopic(ctx, sub.Config.Topic); err != nil {
				return fmt.Errorf("fail to create automatic topic %q: %w", sub.Config.Topic, err)
			}
			return s.getOrCreateSubscription(ctx, sub)
		}
	}

	sub.subscription = subscription
	return nil
}

func (s *Subscription) pullMessage(ctx context.Context, sub *Subscriber) {
	response, err := s.client.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: sub.subscription.Name,
		MaxMessages:  sub.Config.PullingQuantity,
	})
	if err != nil {
		if ErrCodeAlias(status.Convert(err).Code()).IsDeadLineOrCanceledError() {
			return
		}
	}
	if len(response.ReceivedMessages) == 0 {
		return
	}

	for _, message := range response.ReceivedMessages {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if recoveryError, ok := r.(error); ok {
						sub.OnError(ctx, recoveryError, message.Message.Attributes)
						return
					}
					if recoveryError, ok := r.(string); ok {
						sub.OnError(ctx, errors.New(recoveryError), message.Message.Attributes)
						return
					}
				}
			}()
		}()

		if sub.Config.EarlyAck {
			_ = s.client.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
				Subscription: sub.subscription.Name,
				AckIds:       []string{message.AckId},
			})
		}
		if err := sub.Handler(ctx, &Message{
			Topic:       sub.Config.Topic,
			Content:     Content{data: message.Message.Data},
			OriginalKey: message.Message.OrderingKey,
			Headers:     message.Message.Attributes,
		}); err != nil {
			sub.OnError(ctx, err, message.Message.Attributes)
			var pubSubError PubsubError
			if errors.As(err, &pubSubError) {
				if pubSubError.Retriable() {
					return
				}
			}
		}
		_ = s.client.Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
			Subscription: sub.subscription.Name,
			AckIds:       []string{message.AckId},
		})
	}
}

func (s *Subscription) Close(_ context.Context) error {
	s.subscribers = nil
	return s.client.Close()
}
