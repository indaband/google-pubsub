package google_pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/pubsub"
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

type (
	PublisherConfig struct {
		ProjectID string
	}

	Publisher struct {
		config    PublisherConfig
		provider  *pubsub.Client
		validator *validator.Validate
	}

	Content struct {
		data    []byte
		rawData any
	}

	Header  map[string]string
	Message struct {
		// Enable to identify message, if order must be respect this field it's used then.
		OriginalKey string
		Topic       string  `validate:"required"`
		Content     Content `validate:"required"`
		Headers     Header
	}
)

func NewContent(data any) Content { return Content{rawData: data} }

func (c Content) Marshal() (Content, error) {
	data, err := json.Marshal(c.rawData)
	c.data = data
	return c, err
}

func (c Content) Unmarshal(to any) error {
	return json.Unmarshal(c.data, &to)
}

func (p Publisher) Send(ctx context.Context, message *Message) error {
	if err := p.validator.StructCtx(ctx, message); err != nil {
		return fmt.Errorf("%w: %v", ErrValidation, err)
	}

	request := pubsub.Message{
		ID:          uuid.New().String(),
		Data:        message.Content.data,
		Attributes:  message.Headers,
		OrderingKey: message.OriginalKey,
	}

	if message.Content.data == nil {
		content, err := message.Content.Marshal()
		if err != nil {
			return err
		}
		request.Data = content.data
	}

	if _, err := p.provider.
		TopicInProject(message.Topic, p.config.ProjectID).
		Publish(ctx, &request).
		Get(ctx); err != nil {
		return fmt.Errorf(
			"source error: fail to publish message on topic %q with project %q: %w",
			message.Topic, p.config.ProjectID, err,
		)
	}

	return nil
}

func (p Publisher) Close(_ context.Context) error {
	return p.provider.Close()
}
