package v1alpha1

// These constants define valid event severity values.
const (
	// EventSeverityTrace represents a trace event, usually
	// informing about actions taken during reconciliation.
	EventSeverityTrace string = "trace"
	// EventSeverityInfo represents an informational event, usually
	// informing about changes.
	EventSeverityInfo string = "info"
	// EventSeverityError represent an error event, usually a warning
	// that something goes wrong.
	EventSeverityError string = "error"
)

// EventTypeTrace represents a trace event.
const EventTypeTrace string = "Trace"
