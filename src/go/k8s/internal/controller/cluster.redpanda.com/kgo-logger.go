package clusterredpandacom

import (
	"github.com/go-logr/logr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// KgoZapLogger is a franz-go logger adapter for zap.
type KgoZapLogger struct {
	logger logr.Logger
}

// Level Implements kgo.Logger interface. It returns the log level to log at.
// We pin this to debug as the zap logger decides what to actually send to the output stream.
func (KgoZapLogger) Level() kgo.LogLevel {
	return kgo.LogLevelDebug
}

// Log implements kgo.Logger interface
func (k KgoZapLogger) Log(level kgo.LogLevel, msg string, keyvals ...interface{}) {
	switch level {
	case kgo.LogLevelNone:
		// Don't log anything.
	case kgo.LogLevelDebug:
		k.logger.V(TraceLevel).Info(msg, keyvals...)
	case kgo.LogLevelInfo:
		k.logger.V(DebugLevel).Info(msg, keyvals...)
	case kgo.LogLevelWarn:
		k.logger.Info(msg, keyvals...)
	case kgo.LogLevelError:
		k.logger.Error(nil, msg, keyvals...)
	}
}
