package eventtest

import (
	"testing"

	"github.com/stretchr/testify/mock"
)

type MockListener struct {
	mock.Mock
}

func NewMockListener(t *testing.T) *MockListener {
	m := new(MockListener)
	t.Cleanup(func() {
		m.AssertExpectations(t)
	})
	return m
}

func (m *MockListener) EventName() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockListener) GroupID() string {
	args := m.Called()
	return args.String(0)
}

func (m *MockListener) Caller(data []byte, metadata map[string]string) error {
	args := m.Called(data, metadata)
	return args.Error(0)
}

func (m *MockListener) OnSuccess(metadata map[string]string) {
	m.Called(metadata)
}

func (m *MockListener) OnError(err error, metadata map[string]string) {
	m.Called(err, metadata)
}

type MockNotifier struct {
	mock.Mock
}

func NewMockNotifier(t *testing.T) *MockNotifier {
	m := new(MockNotifier)
	t.Cleanup(func() {
		m.AssertExpectations(t)
	})
	return m
}

func (mn *MockNotifier) Dispatch(name string, data []byte, metadata map[string]string) error {
	args := mn.Called(name, data, metadata)
	return args.Error(0)
}
func (mn *MockNotifier) Close() { mn.Called() }
