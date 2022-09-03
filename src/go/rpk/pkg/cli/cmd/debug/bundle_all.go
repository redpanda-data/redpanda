// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build !linux

package debug

import (
	"context"
	"errors"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/twmb/franz-go/pkg/kgo"
)

func executeBundle(
	context.Context,
	afero.Fs,
	*config.Config,
	*kgo.Client,
	*admin.AdminAPI,
	string, string,
	int,
	time.Duration,
) error {
	return errors.New("rpk debug bundle is unsupported on your operating system")
}
