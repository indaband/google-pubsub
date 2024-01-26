package google_pubsub

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	pubsub "cloud.google.com/go/pubsub/apiv1"
	"github.com/go-playground/validator/v10"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// PubSub implements notifier interface
type (
	PubSub struct {
		publisherClient  Publisher
		subscriberClient *Subscription
		config           *PubSubConfig
	}

	PubSubConfig struct {
		ProjectID string
	}
)

// Close all remain connections and release resource
func (p *PubSub) Close(ctx context.Context) error {
	if err := p.publisherClient.Close(ctx); err != nil {
		return err
	}
	return p.subscriberClient.Close(ctx)
}

// PubSubOptions enable to customize configuration
type PubSubOptions func(*PubSubConfig)

// LoadFromEnv load from environment needed information to connect
func LoadFromEnv(pbc *PubSubConfig) {
	pbc.ProjectID = os.Getenv("PROJECT_ID")
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
		publisherClient: Publisher{
			config:    PublisherConfig{ProjectID: config.ProjectID},
			provider:  pubc,
			validator: validator.New(),
		},
		subscriberClient: &Subscription{
			subscribers: make(Subscribers, 0, 100),
			projectID:   config.ProjectID,
			client:      subc,
		},
		config: &config,
	}, nil
}

func (p *PubSub) Publisher() Publisher        { return p.publisherClient }
func (p *PubSub) Subscription() *Subscription { return p.subscriberClient }
