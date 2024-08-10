// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

type pluginHelp struct {
	Path    string   `json:"path,omitempty"`
	Short   string   `json:"short,omitempty"`
	Long    string   `json:"long,omitempty"`
	Example string   `json:"example,omitempty"`
	Args    []string `json:"args,omitempty"`
}

// In support of rpk.ac --help-autocomplete.
func traverseHelp(cmd *cobra.Command, pieces []string) []pluginHelp {
	pieces = append(pieces, strings.Split(cmd.Use, " ")[0])
	help := []pluginHelp{{
		Path:    strings.Join(pieces, "_"),
		Short:   cmd.Short,
		Long:    cmd.Long,
		Example: cmd.Example,
		Args:    cmd.ValidArgs,
	}}
	for _, cmd := range cmd.Commands() {
		help = append(help, traverseHelp(cmd, pieces)...)
	}
	return help
}

func main() {
	var helpAutocomplete bool
	root := cobra.Command{
		Use: "pluginmock",
		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			if helpAutocomplete {
				json.NewEncoder(os.Stdout).Encode(traverseHelp(cmd, nil))
				os.Exit(0)
			}
		},
		Run: func(root *cobra.Command, _ []string) {
			// The root command needs a "run", otherwise executing
			// --help-autocomplete with no args will not actually
			// do anything...
			root.Help()
		},
	}
	root.PersistentFlags().BoolVar(&helpAutocomplete, "help-autocomplete", false, "autocompletion help for rpk")
	root.PersistentFlags().MarkHidden("help-autocomplete")

	root.AddCommand(
		newRunCommand(),
	)

	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

type flag struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}
type output struct {
	Args  []string `json:"args"`
	Flags []flag   `json:"flags"` // We prefer a list, instead of a map to check for repeated values.
}

func newRunCommand() *cobra.Command {
	return &cobra.Command{
		Use: "run",
		FParseErrWhitelist: cobra.FParseErrWhitelist{
			// Allow unknown flags so that arbitrary flags can be passed
			// through. In the actual plugin implementations we add these
			// flags individually but here we just want to know what's being
			// passed by rpk
			UnknownFlags: true,
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			f := collectFlags(os.Args)
			o := output{
				Args:  args,
				Flags: f,
			}
			b, err := json.Marshal(o)
			if err != nil {
				return err
			}
			fmt.Println(string(b))
			return nil
		},
	}
}

func collectFlags(args []string) []flag {
	var flags []flag
	i := 0
	for i < len(args)-1 {
		if strings.HasPrefix(args[i], "-") {
			flags = append(flags, flag{args[i], args[i+1]})
		}
		i++
	}
	return flags
}
