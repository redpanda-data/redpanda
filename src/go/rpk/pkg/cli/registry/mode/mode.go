// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package mode

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/sr"
)

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "mode",
		Short: "Manage schema registry mode",
		Args:  cobra.ExactArgs(0),
	}
	cmd.AddCommand(
		getCommand(fs, p),
		resetCommand(fs, p),
		setCommand(fs, p),
	)
	p.InstallFormatFlag(cmd)
	return cmd
}

type modeResult struct {
	Subject string `json:"subject,omitempty" yaml:"subject,omitempty"`
	Mode    string `json:"mode,omitempty" yaml:"mode,omitempty"`
	ErrMsg  string `json:"error,omitempty" yaml:"error,omitempty"`
}

// printModeResult prints the []sr.ModeResult in the given format, it replaces
// empty subjects for {GLOBAL}, and returns a boolean if encounters an error
// in the ModeResult slice.
func printModeResult(f config.OutFormatter, res []sr.ModeResult) (bool, error) {
	var (
		response []modeResult
		exit1    bool
	)
	for _, r := range res {
		result := modeResult{Mode: r.Mode.String(), Subject: r.Subject}
		if r.Subject == "" {
			result.Subject = "{GLOBAL}"
		}
		if r.Err != nil {
			result.ErrMsg = r.Err.Error()
			// If there is an error, the mode can be empty (0) and it defaults
			// to IMPORT in the sr package. We override that to avoid confusion.
			result.Mode = "-"
			exit1 = true
		}
		response = append(response, result)
	}
	if isText, _, s, err := f.Format(response); !isText {
		if err != nil {
			return exit1, fmt.Errorf("unable to print in the required format %q: %v", f.Kind, err)
		}
		fmt.Println(s)
		return exit1, nil
	}
	tw := out.NewTable("subject", "mode", "error")
	defer tw.Flush()
	for _, r := range response {
		tw.PrintStructFields(r)
	}
	return exit1, nil
}
