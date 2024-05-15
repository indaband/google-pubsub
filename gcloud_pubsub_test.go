package google_pubsub_test

import (
	"context"
	"os"
	"testing"
	"time"

	google_pubsub "github.com/indaband/google-pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/gcloud"
)

func Test_Register_Subscription(t *testing.T) {
	ctx := context.TODO()

	container, err := gcloud.RunPubsubContainer(
		ctx,
		testcontainers.WithImage("gcr.io/google.com/cloudsdktool/cloud-sdk:367.0.0-emulators"),
		gcloud.WithProjectID("pubsub-fake-project"),
	)
	require.NoError(t, err)
	_ = os.Setenv("PUBSUB_EMULATOR_HOST", container.URI)

	t.Cleanup(func() {
		os.Clearenv()
		require.NoError(t, container.Terminate(ctx))
	})

	t.Run("it should receive messages from desired topic", func(t *testing.T) {
		pubsub, err := google_pubsub.ConnectPubSub(func(config *google_pubsub.PubSubConfig) {
			config.ProjectID = container.Settings.ProjectID
		})

		require.NoError(t, err)
		err = pubsub.Register(ctx, &google_pubsub.Subscriber{
			Config: google_pubsub.SubscriptionConfig{
				SubscriptionID:            "fake-one-subscription",
				Topics:                    []string{"fake-topic-1"},
				EnableExactlyOnceDelivery: true,
				AckDeadline:               time.Minute,
			},
			Handler: func(ctx context.Context, message *google_pubsub.Message) error {
				assert.Equal(t, "fake-topic-1", message.Topic)
				var data map[string]any
				require.NoError(t, message.Content.Unmarshal(&data))
				assert.Equal(t, map[string]any{"data": "ok"}, data)
				assert.Equal(t, google_pubsub.Header{"custom": "header"}, message.Headers)
				return nil
			},
			OnError: func(ctx context.Context, err error, header google_pubsub.Header) {
				assert.Equal(t, google_pubsub.Header{
					"location": "registration subscription: fake-one-subscription from topic [fake-topic-1]",
				}, header)
				assert.NoError(t, err)
			},
		})
		require.NoError(t, err)
		time.Sleep(time.Second * 5)
		err = pubsub.Send(ctx, &google_pubsub.Message{
			Topic:   "fake-topic-1",
			Headers: map[string]string{"custom": "header"},
			Content: google_pubsub.NewContent(map[string]any{"data": "ok"}),
		})
		require.NoError(t, err)
	})

	t.Run("it should be able to subscribe to more than one topic at once with one subscriber", func(t *testing.T) {
		pubsub, err := google_pubsub.ConnectPubSub(func(config *google_pubsub.PubSubConfig) {
			config.ProjectID = container.Settings.ProjectID
		})

		require.NoError(t, err)
		err = pubsub.Register(ctx, &google_pubsub.Subscriber{
			Config: google_pubsub.SubscriptionConfig{
				SubscriptionID:            "fake-subscription",
				Topics:                    []string{"topic-1", "topic-2"},
				EnableExactlyOnceDelivery: true,
				AckDeadline:               time.Minute,
			},
			Handler: func(ctx context.Context, message *google_pubsub.Message) error {
				assert.Equal(t, "topic-1", message.Topic)
				var data map[string]any
				require.NoError(t, message.Content.Unmarshal(&data))
				assert.Equal(t, map[string]any{"data": "ok"}, data)
				assert.Equal(t, google_pubsub.Header{"custom": "header"}, message.Headers)
				return nil
			},
			OnError: func(ctx context.Context, err error, header google_pubsub.Header) {
				assert.Equal(t, google_pubsub.Header{
					"location": "registration subscription: fake-subscription-0 from topic [topic-1]",
				}, header)
				assert.NoError(t, err)
			},
		})
		require.NoError(t, err)
		time.Sleep(time.Second * 5)
		err = pubsub.Send(ctx, &google_pubsub.Message{
			Topic:   "topic-1",
			Headers: map[string]string{"custom": "header"},
			Content: google_pubsub.NewContent(map[string]any{"data": "ok"}),
		})

		err = pubsub.Send(ctx, &google_pubsub.Message{
			Topic:   "topic-2",
			Headers: map[string]string{"custom": "header"},
			Content: google_pubsub.NewContent(map[string]any{"data": "ok"}),
		})
		require.NoError(t, err)
	})
}
