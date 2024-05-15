package google_pubsub

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	pubsub "cloud.google.com/go/pubsub"
	"github.com/go-playground/validator/v10"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// LegacyPubSub implements notifier interface
type (
	LegacyPubSub struct {
		publisherClient  Publisher
		subscriberClient *Subscription
		listeners        []AdvancedPubSubListener
		config           *PubSubConfig
	}

	PubSubConfig struct {
		ProjectID       string
		DispatchTimeout time.Duration
		tickerTime      time.Duration
		//Deprecated: PullingQuantity is no longer needed, since we are streaming message instead.
		PullingQuantity int32
	}
)

// Deprecated: Dispatch PLEASE DO NOT use it anymore. Instead, call Send(ctx context.Context, message *Message) error
func (p *LegacyPubSub) Dispatch(name string, data []byte, metadata map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), p.config.DispatchTimeout)
	defer cancel()
	return p.publisherClient.Send(ctx, &Message{
		Topic:   name,
		Headers: metadata,
		Content: NewContent(data),
	})
}

func (p *LegacyPubSub) Send(ctx context.Context, message *Message) error {
	return p.publisherClient.Send(ctx, message)
}

// Close all remain connections and release resource
func (p *LegacyPubSub) Close() {
	_ = p.publisherClient.Close(context.Background())
	_ = p.subscriberClient.Close(context.Background())
}

// Deprecated: Subscribe please stop using it instead, call Register(ctx context.Context, sub ...*Subscribers)error
func (p *LegacyPubSub) Subscribe(ctx context.Context, listeners ...Listener) error {
	subscribers := make([]*Subscriber, len(listeners))
	for _, listener := range listeners {
		subscribers = append(subscribers, &Subscriber{
			Config: SubscriptionConfig{
				AckDeadline:               time.Minute,
				EarlyAck:                  false,
				Topics:                    []string{listener.EventName()},
				SubscriptionID:            listener.GroupID(),
				EnableExactlyOnceDelivery: true,
			},
		})
	}

	return p.subscriberClient.Register(ctx, subscribers...)
}

// Deprecated: SubscribeWithEnhancement allow to include advanced configuration on listener such as early ack options.
// Please DO NOT USE IT ANYMORE, instead use Register(ctx context.Context, sub ...*Subscribers)error
func (p *LegacyPubSub) SubscribeWithEnhancement(ctx context.Context, listeners ...AdvancedPubSubListener) error {
	subscribers := make([]*Subscriber, len(listeners))
	for _, listener := range listeners {
		subscribers = append(subscribers, &Subscriber{
			Config: SubscriptionConfig{
				AckDeadline:               time.Minute,
				EarlyAck:                  listener.EarlyAckEnabled(),
				Topics:                    []string{listener.EventName()},
				SubscriptionID:            listener.GroupID(),
				EnableExactlyOnceDelivery: true,
			},
		})
	}
	return p.subscriberClient.Register(ctx, subscribers...)
}

func (p *LegacyPubSub) Register(ctx context.Context, subscribers ...*Subscriber) error {
	return p.subscriberClient.Register(ctx, subscribers...)
}

// PubSubOptions enable to customize configuration
type PubSubOptions func(*PubSubConfig)

// LoadFromEnv load from environment needed information to connect
func LoadFromEnv(pbc *PubSubConfig) {
	pbc.ProjectID = os.Getenv("PROJECT_ID")
	pbc.DispatchTimeout = String().ParseToDuration(os.Getenv("EVENT_DISPATCH_TIMEOUT"), time.Millisecond*500)
	pbc.tickerTime = String().ParseToDuration(os.Getenv("EVENT_PULL_TICKER_TIME"), time.Second)
	pbc.PullingQuantity = int32(String().ParseToInt(os.Getenv("EVENT_PULLING_SIZE"), 30))
}

// ConnectPubSub enable to connect data from it
func ConnectPubSub(opts ...PubSubOptions) (*LegacyPubSub, error) {
	var config PubSubConfig
	for _, opt := range opts {
		opt(&config)
	}

	var o []option.ClientOption
	// Environment variables for gcloud emulator:
	// https://cloud.google.com/sdk/gcloud/reference/beta/emulators/pubsub/
	if addr := os.Getenv("PUBSUB_EMULATOR_HOST"); addr != "" {
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

	client, err := pubsub.NewClient(context.Background(), config.ProjectID, o...)
	if err != nil {
		return nil, fmt.Errorf("pubsub(client): %w", err)
	}

	return &LegacyPubSub{
		publisherClient: Publisher{
			config:    PublisherConfig{ProjectID: config.ProjectID},
			provider:  client,
			validator: validator.New(),
		},
		subscriberClient: &Subscription{
			subscribers: make(Subscribers, 0, 100),
			projectID:   config.ProjectID,
			client:      client,
		},
		config:    &config,
		listeners: make([]AdvancedPubSubListener, 0, 100),
	}, nil
}
