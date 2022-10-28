// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package auth

import (
	"fmt"
	"os"
	"strings"
)

const (
	FlagClientID     = "client-id"
	FlagClientSecret = "client-secret"

	EnvClientID     = "CLOUD_CLIENT_ID"
	EnvClientSecret = "CLOUD_CLIENT_SECRET"
)

// Params represents the information passed to the cloud command.
type Params struct {
	ClientID     string
	ClientSecret string
}

// EnvOverride check if the flags where set, if not, sets the params value
// with the corresponding env variable.
func (p *Params) EnvOverride() {
	// Flags are set, we don't need to check for env variables
	if p.ClientID != "" || p.ClientSecret != "" {
		return
	}

	if ok := checkEnvTogether(EnvClientID, EnvClientSecret); !ok {
		// Either no envs were set, or we only set one of them.
		return
	}

	// These are the override functions, maps env variables with the
	// corresponding param attribute.
	for k, fn := range map[string]func(string){
		EnvClientID:     func(v string) { p.ClientID = v },
		EnvClientSecret: func(v string) { p.ClientSecret = v },
	} {
		fn(os.Getenv(k))
	}
}

// checkEnvTogether checks if all the passed environment variables are set
// together, return true if all are set together, otherwise, false.
// Prints a warning if we set n-1 variables.
func checkEnvTogether(envs ...string) bool {
	var set, notSet []string
	for _, env := range envs {
		if _, ok := os.LookupEnv(env); ok {
			set = append(set, env)
		} else {
			notSet = append(notSet, env)
		}
	}

	if len(set) == len(envs) {
		return true
	}

	if len(set) > 0 {
		fmt.Printf("if any environment variable in the group [%s] are set they must be all set; missing [%s]\n", strings.Join(envs, ", "), strings.Join(notSet, ","))
		return false
	}

	return false
}
