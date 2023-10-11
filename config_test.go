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

func TestStringEnv_ParseToInt(t *testing.T) {
	type args struct {
		value        string
		defaultValue int
	}
	tests := []struct {
		name string
		s    gp.StringEnv
		args args
		want int
	}{
		{
			name: "it should convert to given number",
			s:    gp.String(),
			args: args{
				value:        "10",
				defaultValue: 1,
			},
			want: 10,
		},
		{
			name: "it should grab default value in case of invalid one",
			s:    gp.String(),
			args: args{
				value:        "fake",
				defaultValue: 1,
			},
			want: 1,
		},
		{
			name: "it should grab default value in case empty value",
			s:    gp.String(),
			args: args{
				value:        "",
				defaultValue: 21,
			},
			want: 21,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.s.ParseToInt(tt.args.value, tt.args.defaultValue), "ParseToInt(%v, %v)", tt.args.value, tt.args.defaultValue)
		})
	}
}
