// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package context

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
)

func TestSetPossibilities(t *testing.T) {
	var cx config.RpkContext
	toSet := map[string]string{
		"kafka_api.brokers":       "127.0.0.1,127.0.0.1",
		"kafka_api.tls.enabled":   "true",
		"kafka_api.tls.ca_file":   "/foo/ca",
		"kafka_api.tls.cert_file": "/foo/cert",
		"kafka_api.tls.key_file":  "/foo/key",
		"kafka_api.sasl.user":     "user",
		"kafka_api.sasl.password": "pass",
		"kafka_api.sasl.type":     "scram-sha-256",
		"admin_api.addresses":     "localhost:9644",
		"admin_api.tls.enabled":   "{}",
		"admin_api.tls.ca_file":   "/foo/ca",
		"admin_api.tls.cert_file": "/foo/cert",
		"admin_api.tls.key_file":  "/foo/key",
	}
	for _, p := range setPossibilities {
		set, ok := toSet[p]
		if !ok {
			t.Errorf("test toSet is missing set possibility %q", p)
			continue
		}
		err := config.Set(&cx, p, set)
		if err != nil {
			t.Errorf("toSet %q value %q failed: %v", p, set, err)
		}
	}
}
