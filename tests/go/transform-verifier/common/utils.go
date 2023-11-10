/*
* Copyright 2023 Redpanda Data, Inc.
*
* Use of this software is governed by the Business Source License
* included in the file licenses/BSL.md
*
* As of the Change Date specified in that file, in accordance with
* the Business Source License, use of this software will be governed
* by the Apache License, Version 2.0
 */

package common

import (
	"context"
	"fmt"
	"os"
)

func Die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

type (
	cancelContextKeyType struct{}
	reporterKeyType      struct{}
	statusReporterHolder struct {
		value *StatusReporter
	}
)

var (
	cancelContextKey = cancelContextKeyType{}
	reporterKey      = reporterKeyType{}
)

func MakeRootContext() context.Context {
	root := context.Background()
	root, cancel := context.WithCancel(root)
	root = context.WithValue(root, cancelContextKey, cancel)
	root = context.WithValue(root, reporterKey, &statusReporterHolder{})
	return root
}

func CancelContext(ctx context.Context) {
	fn := ctx.Value(cancelContextKey).(context.CancelFunc)
	fn()
}

func SetStatusReporter(ctx context.Context, sr *StatusReporter) {
	holder := ctx.Value(reporterKey).(*statusReporterHolder)
	holder.value = sr
}

func ReportStatus(ctx context.Context, val any) {
	holder := ctx.Value(reporterKey).(*statusReporterHolder)
	holder.value.Set(val)
}
