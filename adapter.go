package google_pubsub

type (
	EnhancedListener struct {
		listener        Listener
		earlyAckEnabled bool
	}
)

func NewEnhancedListener(listener Listener, enableEarlyAck bool) EnhancedListener {
	return EnhancedListener{
		listener:        listener,
		earlyAckEnabled: enableEarlyAck,
	}
}

func (e EnhancedListener) EventName() string {
	return e.listener.EventName()
}

func (e EnhancedListener) GroupID() string {
	return e.listener.GroupID()
}

func (e EnhancedListener) Caller(bytes []byte, m map[string]string) error {
	return e.listener.Caller(bytes, m)
}

func (e EnhancedListener) OnSuccess(metadata map[string]string) {
	e.listener.OnSuccess(metadata)
}

func (e EnhancedListener) OnError(err error, metadata map[string]string) {
	e.listener.OnError(err, metadata)
}
func (e EnhancedListener) EarlyAckEnabled() bool { return e.earlyAckEnabled }
