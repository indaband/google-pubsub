package google_pubsub_test

import (
	"testing"
	"time"

	gp "github.com/indaband/google-pubsub"
	"github.com/stretchr/testify/assert"
)

func TestConfigEvent(t *testing.T) {
	t.Run("StringEnvSuiteCase", StringEnvSuiteCase)
}

func StringEnvSuiteCase(t *testing.T) {
	t.Run("should return time.Duration type from string value", func(t *testing.T) {
		duration := gp.String().ParseToDuration("3s", time.Second)
		assert.Equal(t, time.Second*3, duration)
	})

	t.Run("should return default value from it fails to parse given value", func(t *testing.T) {
		duration := gp.String().ParseToDuration("invalid-value", time.Second)
		assert.Equal(t, time.Second, duration)
	})
	t.Run("should return default value from it if value is blank given value", func(t *testing.T) {
		duration := gp.String().ParseToDuration("", time.Second)
		assert.Equal(t, time.Second, duration)
	})
}
