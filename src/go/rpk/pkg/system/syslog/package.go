// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package syslog is necessary for non-linux build, since all
// other files in this package are only built for linux, would which cause
// darwin builds to fail.
package syslog
