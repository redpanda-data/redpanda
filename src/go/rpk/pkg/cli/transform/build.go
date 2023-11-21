// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package transform

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/buildpack"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/transform/project"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newBuildCommand(fs afero.Fs, execFn func(string, []string) error) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build",
		Short: "Build a transform",
		Long: `Build a transform.

This command looks in the current working directory for a transform.yaml file.
It installs the appropriate build plugin, then builds a .wasm file.

When invoked, it passes extra arguments directly to the underlying toolchain.

For example to add debug symbols and use the asyncify scheduler for tinygo:

  rpk transform build -- -scheduler=asyncify -no-debug=false

LANGUAGES

Tinygo - By default tinygo uses release builds (-opt=2) and goroutines are 
disabled, for maximum performance. To enable goroutines, pass 
-scheduler=asyncify to the underlying build command.
`,
		Args: cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, extraArgs []string) {
			cfg, err := project.LoadCfg(fs)
			out.MaybeDie(err, "unable to find the transform, are you in the same directory as the %q?", project.ConfigFileName)
			switch cfg.Language {
			case project.WasmLangTinygo:
				tinygo, err := buildpack.Tinygo.Install(cmd.Context(), fs)
				out.MaybeDieErr(err)
				// See https://tinygo.org/docs/guides/optimizing-binaries/
				args := []string{
					"build",
					// We're targeting WASI environments
					"-target", "wasi",
					// Optimize these binaries to the max!
					// We want LLVM to use all it's tricks to
					// make our code fast.
					"-opt", "2",
					// Print out an error before aborting, this
					// greatly aids debugging.
					"-panic", "print",
					// The default scheduler is asyncify, which uses
					// Binaryen’s Asyncify Pass to enable goroutine
					// and channel support. However, that makes code
					// the code 10x slower in our benchmarks, so
					// default to that off.
					"-scheduler", "none",
					// Debug symbols can make the size of binaries to be
					// an order of magnitude larger, so we remove them.
					"-no-debug",
				}
				// Tinygo's flags can be overridden, so passing flags through
				// allows the user to customize the defaults (i.e. turn on debug
				// symbols or the scheduler).
				args = append(args, extraArgs...)
				// Output using the project name, deploy expects this format.
				args = append(args, "-o", fmt.Sprintf("%s.wasm", cfg.Name))
				out.MaybeDieErr(execFn(tinygo, args))
			default:
				out.Die("unknown language: %q", cfg.Language)
			}
		},
	}
	return cmd
}
