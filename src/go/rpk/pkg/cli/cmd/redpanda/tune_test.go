// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package redpanda

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

func TestInteractive(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedOutput string
		expectErr      bool
	}{
		{
			name:  "it should return if the user enters 'n'",
			input: "n\n",
		},
		{
			name:           "it should exit if the user enters 'q'",
			input:          "q\n",
			expectedOutput: "user exited",
			expectErr:      true,
		},
		{
			name:           "it should prompt again if the user enters something else",
			input:          "what\ny\n",
			expectedOutput: "Unrecognized option 'what'",
		},
		{
			name:           "it should prompt again if the user just hits 'return'",
			input:          "\ny\n",
			expectedOutput: "Please choose an option",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := config.Default()
			fs := afero.NewMemMapFs()
			mgr := config.NewManager(fs)
			// Write invalid yaml to simulate an error
			_, err := utils.WriteBytes(fs, []byte("*asdf&"), conf.ConfigFile)
			if err != nil {
				log.Fatal(err)
			}
			_, err = utils.WriteBytes(fs, []byte("60"), "/proc/sys/vm/swappiness")
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			in := strings.NewReader(tt.input)
			cmd := NewTuneCommand(fs, mgr)
			cmd.SetArgs([]string{"swappiness", "--interactive"})
			cmd.SetIn(in)
			logrus.SetOutput(&out)
			err = cmd.Execute()
			if tt.expectErr {
				if err == nil {
					t.Fatal("Expected an error, but got nil")
				}
				return
			} else {
				if err != nil {
					t.Fatalf("Got an unexpected error: %v", err)
				}
			}
			output := out.String()
			if !strings.Contains(output, tt.expectedOutput) {
				t.Fatalf(
					"expected output:\n\"%s\"\nto contain\n\"%s\"",
					output,
					tt.expectedOutput,
				)
			}
		})
	}
}
