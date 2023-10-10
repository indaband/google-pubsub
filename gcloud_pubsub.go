package google_pubsub

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"time"

	pubsubV0 "cloud.google.com/go/pubsub"
	pubsub "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"github.com/google/uuid"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// PubSub implements notifier interface
type (
	PubSub struct {
		publisherClient  *pubsub.PublisherClient
		subscriberClient *pubsub.SubscriberClient
		listeners        []Listener
		config           *PubSubConfig
	}

	PubSubConfig struct {
		ProjectID       string
		DispatchTimeout time.Duration
		tickerTime      time.Duration
	}
)

func (p *PubSub) Dispatch(name string, data []byte, metadata map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.config.DispatchTimeout)
	defer cancel()
	channel := make(chan struct{ err error }, 1)
	go func() {
		defer close(channel)
		topic := fmt.Sprintf("projects/%s/topics/%s", p.config.ProjectID, name)
		req := pubsubpb.PublishRequest{
			Topic: topic,
			Messages: []*pubsubpb.PubsubMessage{
				{
					MessageId:  uuid.New().String(),
					Data:       data,
					Attributes: metadata,
				},
			},
		}

		_, err := p.publisherClient.Publish(ctx, &req)
		channel <- struct{ err error }{err}
	}()
	select {
	case result := <-channel:
		return result.err
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return fmt.Errorf("operation timeout, could not dispatch event to topic %s after %s %v", name, p.config.DispatchTimeout.String(), ctx.Err())
		}
		return fmt.Errorf("operation cancelled: could not dispatch event to topic %s after %s: %v", name, p.config.DispatchTimeout.String(), ctx.Err())
	}
}

// Close all remain connections and release resource
func (p *PubSub) Close() {
	_ = p.publisherClient.Close()
	_ = p.subscriberClient.Close()
}

func (p *PubSub) getOrCreateSubscription(ctx context.Context, topic, name string) (*pubsubpb.Subscription, error) {
	subscriptionName := fmt.Sprintf("projects/%s/subscriptions/%s", p.config.ProjectID, name)
	topicFullName := fmt.Sprintf("projects/%s/topics/%s", p.config.ProjectID, topic)

	sub, err := p.subscriberClient.GetSubscription(
		ctx, &pubsubpb.GetSubscriptionRequest{
			Subscription: subscriptionName,
		})

	if err != nil {
		return p.subscriberClient.CreateSubscription(
			ctx, &pubsubpb.Subscription{
				Name:  subscriptionName,
				Topic: topicFullName,
			})
	}
	return sub, nil
}

func (p *PubSub) createTopic(ctx context.Context, l Listener) error {
	cli, err := pubsubV0.
		NewClient(ctx, p.config.ProjectID)

	if err != nil {
		return fmt.Errorf("fail to create topic '%s' %w", l.EventName(), err)
	}

	defer func() { _ = cli.Close() }()

	if _, err := cli.
		CreateTopic(ctx, l.EventName()); err != nil {
		return fmt.Errorf("fail to create topic '%s' %w", l.EventName(), err)
	}

	return nil
}

func (p *PubSub) pullMessages(ctx context.Context, listener Listener, subscription *pubsubpb.Subscription) {
	response, err := p.subscriberClient.Pull(ctx, &pubsubpb.PullRequest{
		Subscription: subscription.Name,
		MaxMessages:  30,
	})
	if err != nil {
		if ErrCodeAlias(status.Convert(err).Code()).IsDeadLineOrCanceledError() {
			return
		}
		listener.
			OnError(
				fmt.Errorf("fail to pull message from subscription '%s' on topic '%s' %w", subscription.Name, subscription.Topic, err),
				map[string]string{},
			)
		return
	}
	if len(response.ReceivedMessages) == 0 {
		return
	}

	for _, message := range response.ReceivedMessages {
		func() {
			defer func(listener Listener) {
				if r := recover(); r != nil {
					if recoveryError, ok := r.(error); ok {
						listener.OnError(recoveryError, message.Message.Attributes)
					} else if errorMsg, ok := r.(string); ok {
						listener.OnError(errors.New(errorMsg), message.Message.Attributes)
					}
				}
			}(listener)
			if err := listener.
				Caller(message.Message.Data, message.Message.Attributes); err != nil {
				listener.
					OnError(err, message.Message.Attributes)
				if ok := errors.Is(err, PubsubError{}); ok {
					var pubSubError PubsubError
					if errors.As(err, &pubSubError) {
						if pubSubError.Retriable() {
							// TODO: figure out how to do with retry. For now on we will not retry any error
							return
						}
					}
				}
				_ = p.subscriberClient.
					Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
						Subscription: subscription.Name,
						AckIds:       []string{message.AckId},
					})
				return
			}
			_ = p.subscriberClient.
				Acknowledge(ctx, &pubsubpb.AcknowledgeRequest{
					Subscription: subscription.Name,
					AckIds:       []string{message.AckId},
				})
			listener.OnSuccess(message.Message.Attributes)
		}()
	}
}

// Subscribe configures listeners to receive messages from a Pub/Sub topic.
// It respects the Listener interface.
func (p *PubSub) Subscribe(ctx context.Context, listeners ...Listener) error {
	p.listeners = append(p.listeners, listeners...)

	for _, listener := range listeners {
		// Launch a new goroutine for each listener.
		go func(l Listener) {
			// Initialize a ticker based on configuration.
			ticker := time.NewTicker(p.config.tickerTime)
			defer ticker.Stop()

			for {
				select {
				// Try to get or create subscription when the ticker fires.
				case <-ticker.C:
					sub, err := p.getOrCreateSubscription(ctx, l.EventName(), l.GroupID())
					if err != nil {
						errCode := status.Convert(err).Code()
						switch errCode {
						case codes.NotFound:
							if createErr := p.
								createTopic(ctx, l); createErr != nil &&
								!ErrCodeAlias(status.Convert(createErr).Code()).AlreadyExists() {
								l.OnError(fmt.Errorf("subscription: %w", createErr), map[string]string{})
							}
						case codes.Canceled:
							// Do nothing for a canceled context.
						default:
							l.OnError(fmt.Errorf("subscription: %w", err), map[string]string{})
						}
						continue
					}
					p.pullMessages(ctx, l, sub)

				// Stop the goroutine if the context is done.
				case <-ctx.Done():
					ticker.Stop()
					_ = p.subscriberClient.Close()
					return
				}
			}
		}(listener)
	}
	return nil
}

// PubSubOptions enable to customize configuration
type PubSubOptions func(*PubSubConfig)

// LoadFromEnv load from environment needed information to connect
func LoadFromEnv(pbc *PubSubConfig) {
	pbc.ProjectID = os.Getenv("PROJECT_ID")
	pbc.DispatchTimeout = String().ParseToDuration(os.Getenv("EVENT_DISPATCH_TIMEOUT"), time.Millisecond*500)
	pbc.tickerTime = String().ParseToDuration(os.Getenv("EVENT_PULL_TICKER_TIME"), time.Second)
}

// ConnectPubSub enable to connect data from it
func ConnectPubSub(opts ...PubSubOptions) (*PubSub, error) {
	var config PubSubConfig
	for _, opt := range opts {
		opt(&config)
	}

	var o []option.ClientOption
	// Environment variables for gcloud emulator:
	// https://cloud.google.com/sdk/gcloud/reference/beta/emulators/pubsub/
	if addr := os.Getenv("PUBSUB_EMULATOR_HOST"); addr != "" {
		fmt.Println(addr)
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("grpc.Dial: %v", err)
		}
		o = []option.ClientOption{option.WithGRPCConn(conn)}
		o = append(o, option.WithTelemetryDisabled())
	} else {
		numConns := runtime.GOMAXPROCS(0)
		if numConns > 4 {
			numConns = 4
		}
		o = []option.ClientOption{
			// Create multiple connections to increase throughput.
			option.WithGRPCConnectionPool(numConns),
			option.WithGRPCDialOption(grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time: 5 * time.Minute,
			})),
		}
	}

	pubc, err := pubsub.NewPublisherClient(context.Background(), o...)
	if err != nil {
		return nil, fmt.Errorf("pubsub(publisher): %v", err)
	}
	subc, err := pubsub.NewSubscriberClient(context.Background(), o...)
	if err != nil {
		return nil, fmt.Errorf("pubsub(subscriber): %v", err)
	}

	return &PubSub{
		publisherClient: pubc, subscriberClient: subc,
		config: &config, listeners: make([]Listener, 0, 20),
	}, nil
}
