package docker

import (
	"context"
	"time"
)

const defaultClientTimeout = 10 * time.Second

func CtxWithDefaultTimeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), defaultClientTimeout)
}
