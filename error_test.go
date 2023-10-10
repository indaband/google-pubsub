package google_pubsub_test

import (
	"errors"
	"testing"

	gp "github.com/indaband/google-pubsub"
	"github.com/stretchr/testify/assert"
)

func TestPubSubError(t *testing.T) {
	t.Run("should return true if error is pubSub error", func(t *testing.T) {
		err := gp.NewPubsubError(errors.New("fake"))
		assert.True(t, errors.Is(err, gp.PubsubError{}))
	})

	t.Run("should return false if error is not pubSub error", func(t *testing.T) {
		assert.False(t, errors.Is(errors.New("other error"), gp.PubsubError{}))
	})

	t.Run("should apply retrievable calling Retry", func(t *testing.T) {
		err := gp.NewPubsubError(errors.New("fake")).Retry()
		assert.True(t, err.Retriable())
	})

	t.Run("should return false to retrievable check by default", func(t *testing.T) {
		err := gp.NewPubsubError(errors.New("fake"))
		assert.False(t, err.Retriable())
	})

	t.Run("should access inner error on error message", func(t *testing.T) {
		err := gp.NewPubsubError(errors.New("fake inner error message")).Retry()
		assert.EqualError(t, err, "fake inner error message")
	})
}
