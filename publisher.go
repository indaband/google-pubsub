package google_pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	pubsub "cloud.google.com/go/pubsub/apiv1"
	"cloud.google.com/go/pubsub/apiv1/pubsubpb"
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

type (
	PublisherConfig struct {
		ProjectID string
	}

	Publisher struct {
		config    PublisherConfig
		provider  *pubsub.PublisherClient
		validator *validator.Validate
	}
	Content struct {
		data []byte
	}

	Header map[string]string

	Message struct {
		// Enable to identify message, if order must be respect this field it's used then.
		OriginalKey string
		Topic       string  `validate:"required"`
		Content     Content `validate:"required"`
		Headers     Header
	}
)

func NewContent() Content { return Content{} }

func (c Content) Marshal(content any) Content {
	data, _ := json.Marshal(content)
	return Content{data: data}
}

func (c Content) Unmarshal(to any) error {
	return json.Unmarshal(c.data, &to)
}

func (p Publisher) Send(ctx context.Context, message *Message) error {
	if err := p.validator.StructCtx(ctx, message); err != nil {
		return fmt.Errorf("validation error: %w", err)
	}
	topic := fmt.Sprintf("projects/%s/topics/%s", p.config.ProjectID, message.Topic)
	request := pubsubpb.PublishRequest{
		Topic: topic,
		Messages: []*pubsubpb.PubsubMessage{
			{
				MessageId:   uuid.New().String(),
				Data:        message.Content.data,
				Attributes:  message.Headers,
				OrderingKey: message.OriginalKey,
			},
		},
	}
	_, err := p.provider.Publish(ctx, &request)
	if err != nil {
		return fmt.Errorf("source error: fail to publish message %w", err)
	}
	return nil
}

func (p Publisher) Close(_ context.Context) error {
	return p.provider.Close()
}
