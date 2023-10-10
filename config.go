package google_pubsub

import "time"

// StringEnv enable to parse string over other type
type StringEnv func()

// String retrieve back a StringEnv type
func String() StringEnv {
	return StringEnv(func() {})
}

// ParseToDuration allow to parse duration
func (s StringEnv) ParseToDuration(value string, defaultValue time.Duration) time.Duration {
	result, err := time.ParseDuration(value)
	if err != nil {
		return defaultValue
	}
	return result
}
