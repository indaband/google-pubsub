package google_pubsub_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	gp "github.com/indaband/google-pubsub"
	"github.com/indaband/google-pubsub/eventtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/gcloud"
)

func TestPubSubSuiteCase(t *testing.T) {
	ctx := context.TODO()

	pubSubContainer, err := gcloud.RunPubsubContainer(
		ctx,
		testcontainers.WithImage("gcr.io/google.com/cloudsdktool/cloud-sdk:367.0.0-emulators"),
		gcloud.WithProjectID("pubsub-project"),
	)

	assert.
		NoError(t, err)

	_ = os.
		Setenv("PUBSUB_EMULATOR_HOST", pubSubContainer.URI)
	_ = os.
		Setenv("PROJECT_ID", "pubsub-project")

	t.Cleanup(func() {
		_ = pubSubContainer.
			Terminate(ctx)
	})

	t.Run("DispatchPubSubSuiteCase", DispatchPubSubSuiteCase)
	t.Run("Subscription", Subscription)
}

func DispatchPubSubSuiteCase(t *testing.T) {
	t.Run("should return timeout after try to connect", func(t *testing.T) {
		notifier, err := gp.ConnectPubSub(func(pc *gp.PubSubConfig) {
			pc.DispatchTimeout = time.Microsecond
		})
		assert.NoError(t, err)
		err = notifier.Dispatch("test", []byte("data"), make(map[string]string))
		assert.EqualError(t, err, "operation timeout, could not dispatch event to topic test after 1µs context deadline exceeded")
	})

	t.Run("should return ERROR if topic does not exist ", func(t *testing.T) {
		notifier, err := gp.ConnectPubSub(func(pc *gp.PubSubConfig) {
			pc.DispatchTimeout = time.Second * 10
			pc.ProjectID = os.Getenv("PROJECT_ID")
		})
		assert.NoError(t, err)
		err = notifier.Dispatch("test_topic_name", []byte("data"), make(map[string]string))
		assert.EqualError(t, err, "rpc error: code = NotFound desc = Topic not found")
	})
}

func Subscription(t *testing.T) {
	_ = os.Setenv("EVENT_PULL_TICKER_TIME", "100ms")
	t.Cleanup(func() {
		_ = os.Setenv("EVENT_PULL_TICKER_TIME", "1s")
	})
	ctx := context.Background()

	t.Run("should subscribe to a topic even when it does not exist", func(t *testing.T) {
		pubSub, err := gp.ConnectPubSub(gp.LoadFromEnv)
		assert.NoError(t, err)
		listenerMock := new(eventtest.MockListener)
		listenerMock.On("EventName").Return("fake-event-name-3")
		listenerMock.On("GroupID").Return("fake-event-group-3")

		err = pubSub.Subscribe(ctx, listenerMock)
		assert.NoError(t, err)
		time.Sleep(time.Second)
		listenerMock.AssertExpectations(t)
	})

	t.Run("should point to an already existing subscription and not return error", func(t *testing.T) {
		pubSub, err := gp.ConnectPubSub(gp.LoadFromEnv)
		assert.NoError(t, err)
		listenerMock := new(eventtest.MockListener)

		listenerMock.On("EventName").Return("fake-event-name")
		listenerMock.On("GroupID").Return("fake-event-group")

		listenerMock.On("Caller", []byte("fake-data"), map[string]string{"x_request_id": "1"}).Return(nil)
		listenerMock.On("OnSuccess", map[string]string{"x_request_id": "1"})

		listenerMock.On("OnError", mock.Anything, map[string]string{})

		err = pubSub.Subscribe(ctx, listenerMock, listenerMock)
		assert.NoError(t, err)
		// first action return error afterward it will get subscription correctly
		time.Sleep(time.Second * 3)

		err = pubSub.Dispatch("fake-event-name", []byte("fake-data"), map[string]string{"x_request_id": "1"})
		assert.NoError(t, err)

		time.Sleep(time.Second)

		listenerMock.AssertNumberOfCalls(t, "Caller", 1)
	})

	t.Run("should call OnError after failure return from caller", func(t *testing.T) {
		pubSub, err := gp.ConnectPubSub(gp.LoadFromEnv)
		assert.NoError(t, err)
		listenerMock := new(eventtest.MockListener)

		listenerMock.On("EventName").Return("fake-event-name-4")
		listenerMock.On("GroupID").Return("fake-event-group-4")

		expectedErr := errors.New("error fake")

		listenerMock.On("Caller", []byte("fake-data"), map[string]string{"x_request_id": "1"}).Return(expectedErr)

		errs := make([]error, 0, 10)
		listenerMock.On("OnError", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			errs = append(errs, args.Error(0))
		})

		err = pubSub.Subscribe(ctx, listenerMock)
		assert.NoError(t, err)
		time.Sleep(time.Second * 3)

		err = pubSub.Dispatch("fake-event-name-4", []byte("fake-data"), map[string]string{"x_request_id": "1"})
		assert.NoError(t, err)

		time.Sleep(time.Second)

		listenerMock.AssertNumberOfCalls(t, "Caller", 1)
		assert.Condition(t, func() (success bool) {
			return assert.Contains(t, errs, expectedErr)
		})
	})

	t.Run("should not call OnError after context cancellation", func(t *testing.T) {
		pubSub, err := gp.ConnectPubSub(gp.LoadFromEnv)
		assert.NoError(t, err)

		cancellableCtx, cancel := context.WithCancel(context.Background())

		listenerMock := new(eventtest.MockListener)
		listenerMock.On("EventName").Return("fake-event-name-3")
		listenerMock.On("GroupID").Return("fake-event-group-3")

		err = pubSub.Subscribe(cancellableCtx, listenerMock)
		assert.NoError(t, err)
		time.Sleep(time.Second * 1)

		err = pubSub.Dispatch("fake-event-name-4", []byte("fake-data"), map[string]string{"x_request_id": "1"})
		assert.NoError(t, err)

		cancel()
		time.Sleep(time.Second)

		listenerMock.AssertNumberOfCalls(t, "Caller", 0)
	})

	t.Run("should recovery panic error in case of it on caller", func(t *testing.T) {
		pubSub, err := gp.ConnectPubSub(gp.LoadFromEnv)
		assert.NoError(t, err)
		listenerMock := new(eventtest.MockListener)

		listenerMock.On("EventName").Return("fake-event-name-10")
		listenerMock.On("GroupID").Return("fake-event-group-10")

		panicError := errors.New("forced panic")
		listenerMock.On("Caller", []byte("fake-data"), map[string]string{"x_request_id": "1"}).Run(func(args mock.Arguments) {
			panic(panicError)
		}).Return(nil)

		errs := make([]error, 0, 10)
		listenerMock.On("OnError", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			errs = append(errs, args.Error(0))
		})

		err = pubSub.Subscribe(ctx, listenerMock)
		assert.NoError(t, err)
		time.Sleep(time.Second * 3)

		err = pubSub.Dispatch("fake-event-name-10", []byte("fake-data"), map[string]string{"x_request_id": "1"})
		assert.NoError(t, err)

		time.Sleep(time.Second)
		listenerMock.AssertNumberOfCalls(t, "Caller", 1)
		assert.Condition(t, func() (success bool) {
			return assert.Contains(t, errs, panicError)
		})
	})

	t.Run("should recovery panic even if panic as string in case of it on caller", func(t *testing.T) {
		pubSub, err := gp.ConnectPubSub(gp.LoadFromEnv)
		assert.NoError(t, err)
		listenerMock := new(eventtest.MockListener)

		listenerMock.On("EventName").Return("fake-event-name-11")
		listenerMock.On("GroupID").Return("fake-event-group-11")

		panicError := errors.New("forced panic")

		listenerMock.On("Caller", []byte("fake-data"), map[string]string{"x_request_id": "1"}).
			Run(func(args mock.Arguments) {
				panic(panicError.Error())
			}).
			Return(nil)

		errs := make([]error, 0, 10)
		listenerMock.On("OnError", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			errs = append(errs, args.Error(0))
		})

		err = pubSub.Subscribe(ctx, listenerMock)
		assert.NoError(t, err)
		time.Sleep(time.Second * 3)

		err = pubSub.Dispatch("fake-event-name-11", []byte("fake-data"), map[string]string{"x_request_id": "1"})
		assert.NoError(t, err)

		time.Sleep(time.Second)
		listenerMock.AssertNumberOfCalls(t, "Caller", 1)
		assert.Condition(t, func() (success bool) {
			return assert.Contains(t, errs, panicError)
		})
	})

	t.Run("should work normally in case of two different subscription on the same topic both must receive incoming message", func(t *testing.T) {
		pubSub, err := gp.ConnectPubSub(gp.LoadFromEnv)
		assert.NoError(t, err)
		listenerMock := new(eventtest.MockListener)
		listenerMock2 := new(eventtest.MockListener)

		listenerMock.On("EventName").Return("fake-event-name-12")
		listenerMock.On("GroupID").Return("fake-event-group-12")

		listenerMock2.On("EventName").Return("fake-event-name-12")
		listenerMock2.On("GroupID").Return("fake-event-group-13")

		// every new subscription and new topic return error but ig goes afterwards
		listenerMock.On("OnError", mock.Anything, mock.Anything)
		listenerMock2.On("OnError", mock.Anything, mock.Anything)

		listenerMock.On("Caller", []byte("fake-data-12"), map[string]string{"x_request_id": "1"}).Return(nil)
		listenerMock2.On("Caller", []byte("fake-data-12"), map[string]string{"x_request_id": "1"}).Return(nil)

		listenerMock.On("OnSuccess", map[string]string{"x_request_id": "1"})
		listenerMock2.On("OnSuccess", map[string]string{"x_request_id": "1"})

		err = pubSub.Subscribe(ctx, listenerMock, listenerMock2)
		assert.NoError(t, err)

		time.Sleep(time.Second * 3)

		err = pubSub.Dispatch("fake-event-name-12", []byte("fake-data-12"), map[string]string{"x_request_id": "1"})
		assert.NoError(t, err)

		time.Sleep(time.Second)

		listenerMock.AssertNumberOfCalls(t, "Caller", 1)
		listenerMock2.AssertNumberOfCalls(t, "Caller", 1)
		listenerMock.AssertNumberOfCalls(t, "OnSuccess", 1)
		listenerMock2.AssertNumberOfCalls(t, "OnSuccess", 1)
	})
}
