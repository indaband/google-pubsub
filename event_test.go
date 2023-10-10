package google_pubsub_test

import (
	"errors"
	"testing"
	"time"

	gp "github.com/indaband/google-pubsub"
	"github.com/indaband/google-pubsub/eventtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestEventSuiteCase(t *testing.T) {
	t.Parallel()
	t.Run("EventCaseNotifiedAndExecutedByListeners", EventCaseNotifiedAndExecutedByListeners)
	t.Run("DispatchSuiteCase", DispatchSuiteCase)
}

func EventCaseNotifiedAndExecutedByListeners(t *testing.T) {
	event := gp.NewEvent(2)
	t.Cleanup(func() {
		event.Close()
	})

	_ = event.Start()

	t.Run("should enable listener to listen to a event name and receive incoming dispatched event properly", func(t *testing.T) {
		mockListener := new(eventtest.MockListener)
		mockListener.On("EventName").Return("test-event")
		mockListener.On("Caller", mock.Anything, mock.Anything).Return(nil)
		mockListener.On("OnSuccess", mock.Anything)

		event.Subscribe(mockListener)

		_ = event.Dispatch("test-event", []byte("my data"), map[string]string{"metadata": "test"})
		time.Sleep(time.Millisecond * 50)
		mockListener.AssertNumberOfCalls(t, "Caller", 1)
		mockListener.AssertNumberOfCalls(t, "OnSuccess", 1)
	})

	t.Run("should call onSuccess passing metadata info to be used", func(t *testing.T) {
		mockListener := new(eventtest.MockListener)
		mockListener.On("EventName").Return("test-event-2")
		mockListener.On("Caller", mock.Anything, mock.Anything).Return(nil)
		mockListener.On("OnSuccess", mock.Anything).Run(func(args mock.Arguments) {
			metadata, ok := args.Get(0).(map[string]string)
			assert.True(t, ok)
			assert.Equal(t, "test-event-2", metadata["eventName"])
		})

		event.Subscribe(mockListener)

		_ = event.Dispatch("test-event-2", []byte("my data"), map[string]string{"eventName": "test-event-2"})
		time.Sleep(time.Millisecond * 50)
		mockListener.AssertNumberOfCalls(t, "Caller", 1)
		mockListener.AssertNumberOfCalls(t, "OnSuccess", 1)
	})

	t.Run("should match params received on caller", func(t *testing.T) {
		mockListener := new(eventtest.MockListener)
		mockListener.On("EventName").Return("test-event-2")
		mockListener.On("Caller", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			data, ok := args.Get(0).([]byte)
			assert.True(t, ok)
			assert.Equal(t, "my data", string(data))
		}).Return(nil)
		mockListener.On("OnSuccess", mock.Anything)
		event.Subscribe(mockListener)

		_ = event.Dispatch("test-event-2", []byte("my data"), map[string]string{"eventName": "test-event-2"})
		time.Sleep(time.Millisecond * 50)
		mockListener.AssertNumberOfCalls(t, "Caller", 1)
		mockListener.AssertNumberOfCalls(t, "OnSuccess", 1)
	})

	t.Run("should start backoff attempts after failure and call caller multiples time til call onError", func(t *testing.T) {
		mockListener := new(eventtest.MockListener)
		mockListener.On("EventName").Return("test-event-with-error")
		mockListener.On("Caller", mock.Anything, mock.Anything).Return(errors.New("mock error"))
		mockListener.On("OnError", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			err := args.Error(0)
			metadata, ok := args.Get(1).(map[string]string)
			assert.True(t, ok)
			assert.EqualError(t, err, "mock error")
			assert.Equal(t, "test-event-with-error", metadata["eventName"])
		})
		event.Subscribe(mockListener)

		_ = event.Dispatch("test-event-with-error", []byte("my data"), map[string]string{"eventName": "test-event-with-error"})
		time.Sleep(time.Second * 2)
		mockListener.AssertNumberOfCalls(t, "Caller", 3)
		mockListener.AssertNotCalled(t, "OnSuccess")
		mockListener.AssertNumberOfCalls(t, "OnError", 1)
	})
}

func DispatchSuiteCase(t *testing.T) {
	t.Run("should not return error to dispatch one event without listeners available to consumer", func(t *testing.T) {
		evt := gp.NewEvent(2)
		defer evt.Close()
		err := evt.Dispatch("my-test", []byte("my  data"), map[string]string{"id": "231"})
		assert.NoError(t, err)
	})

	t.Run("should return error to dispatch more than one event without listeners available to consumer", func(t *testing.T) {
		evt := gp.NewEvent(1)
		defer evt.Close()
		_ = evt.Dispatch("my-test", []byte("my  data"), map[string]string{"id": "234"})
		err := evt.Dispatch("my-test", []byte("my  data"), map[string]string{"id": "231"})
		assert.EqualError(t, err, "there is not listeners available. Channel is busy")
	})
}
