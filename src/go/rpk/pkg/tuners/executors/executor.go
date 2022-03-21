// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package executors

import "github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"

type Executor interface {
	Execute(commands.Command) error
	IsLazy() bool
}
