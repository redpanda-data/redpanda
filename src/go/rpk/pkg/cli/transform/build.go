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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"

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

For example to add debug symbols for tinygo:

  rpk transform build -- -no-debug=false

LANGUAGES

Tinygo - By default tinygo are release builds (-opt=2) for maximum performance.
`,
		Args: cobra.ArbitraryArgs,
		Run: func(cmd *cobra.Command, extraArgs []string) {
			cfg, err := project.LoadCfg(fs)
			out.MaybeDie(err, "unable to find the transform, are you in the same directory as the %q?", project.ConfigFileName)
			switch cfg.Language {
			case project.WasmLangTinygoWithGoroutines:
				fallthrough
			case project.WasmLangTinygoNoGoroutines:
				tinygo, err := buildpack.Tinygo.Install(cmd.Context(), fs)
				out.MaybeDieErr(err)
				var scheduler string
				// The default scheduler is asyncify, which uses Binaryen’s
				// Asyncify Pass to enable goroutine and channel support.
				// However, that makes code the code 10x slower in our
				// benchmarks, so allow disabling it.
				if cfg.Language == project.WasmLangTinygoWithGoroutines {
					scheduler = "asyncify"
				} else {
					scheduler = "none"
				}
				// See https://tinygo.org/docs/guides/optimizing-binaries/
				args := []string{
					"build",
					// We're targeting WASI environments
					"-target", "wasi",
					// Optimize these binaries to the max!
					// We want LLVM to use all it's tricks to
					// make our code fast.
					"-opt", "2",
					// Enable SIMD acceleration!
					"-llvm-features", "+simd128",
					// Print out an error before aborting, this
					// greatly aids debugging.
					"-panic", "print",
					// Use the specified scheduler.
					"-scheduler", scheduler,
					// Debug symbols can make the size of binaries to be
					// an order of magnitude larger, so we remove them.
					"-no-debug",
				}
				// Tinygo's flags can be overridden, so passing flags through
				// allows the user to customize the defaults (i.e. turn on debug
				// symbols).
				args = append(args, extraArgs...)
				// Output using the project name, deploy expects this format.
				args = append(args, "-o", fmt.Sprintf("%s.wasm", cfg.Name))
				out.MaybeDieErr(execFn(tinygo, args))
			case project.WasmLangRust:
				out.MaybeDieErr(buildRust(cmd.Context(), fs, cfg, extraArgs))
			case project.WasmLangTypeScript:
				fallthrough
			case project.WasmLangJavaScript:
				out.MaybeDieErr(buildJavaScript(cmd.Context(), fs, cfg))
			default:
				out.Die("unknown language: %q", cfg.Language)
			}
		},
	}
	return cmd
}

type cargoMetadata struct {
	TargetDir string `json:"target_directory"`
}

func buildRust(ctx context.Context, fs afero.Fs, cfg project.Config, extraArgs []string) error {
	cargo, err := exec.LookPath("cargo")
	if errors.Is(err, exec.ErrNotFound) {
		return fmt.Errorf("cargo is not available on $PATH, please download and install it: https://rustup.rs/")
	} else if err != nil {
		return fmt.Errorf("unable to lookup cargo executable: %v", err)
	}
	cmd := exec.CommandContext(ctx, cargo, "metadata", "--format-version=1")
	var b bytes.Buffer
	cmd.Stdout = &b
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("unable to query cargo metadata: %v", err)
	}
	var meta cargoMetadata
	if err := json.Unmarshal(b.Bytes(), &meta); err != nil {
		return fmt.Errorf("unable to query cargo metadata: %v", err)
	}
	// Cargo does not support named outputs for building, so we have to do the mv ourselves.
	buildArgs := []string{"build", "--release", "--target=wasm32-wasi"}
	buildArgs = append(buildArgs, extraArgs...)
	cmd = exec.CommandContext(ctx, cargo, buildArgs...)
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	// Enable SIMD acceleration
	cmd.Env = append(os.Environ(), "RUSTFLAGS=-Ctarget-feature=+simd128")
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build failed %v", err)
	}
	fileName := fmt.Sprintf("%s.wasm", cfg.Name)
	buildArtifact := path.Join(meta.TargetDir, "wasm32-wasi", "release", fileName)
	if err = fs.Rename(buildArtifact, fileName); err != nil {
		return fmt.Errorf("unable to move build artifact: %v", err)
	}
	return nil
}

const watTemplate = `
(module
	(memory (import "js_vm" "memory") 0)

	(func (export "file_length") (result i32)
          (i32.const %d)
        )

	(func (export "get_file") (param $buffer_ptr i32)
		;; Copy the source from data segment 0.
		(memory.init $source
			(local.get $buffer_ptr) ;; Memory destination
			(i32.const 0)           ;; Start index
			(i32.const %d)          ;; Length
                )
		(data.drop $source)
        )
	(data $source "%s"))
`

// createWatFromJavaScript generates a webassembly file in the text format: http://mdn.io/wasm-text-format
//
// We export two functions in our template, one to get the length of the file, and another to copy the file
// into a buffer. This is an expected from our transform JS SDK.
func createWatFromJavaScript(code []byte) string {
	encoded := hex.EncodeToString(code)
	escaped := strings.Builder{}
	escaped.Grow(len(code) * 3)
	// WebAssembly text format expects each escaped hex character (two ascii letters) to be preceded by a backslash.
	// So that hex a hex string where encoded = "deedbeef", escaped = "\de\ed\be\ef"
	// See the spec here: https://webassembly.github.io/spec/core/text/values.html#text-string
	for i := 0; i < len(encoded); i += 2 {
		escaped.WriteRune('\\')
		escaped.WriteByte(encoded[i])
		escaped.WriteByte(encoded[i+1])
	}
	return fmt.Sprintf(watTemplate, len(code), len(code), escaped.String())
}

func buildJavaScript(ctx context.Context, fs afero.Fs, cfg project.Config) error {
	npm, err := exec.LookPath("npm")
	if errors.Is(err, exec.ErrNotFound) {
		return fmt.Errorf("npm is not available on $PATH, please download and install it: https://nodejs.org/")
	} else if err != nil {
		return fmt.Errorf("unable to lookup npm executable: %v", err)
	}
	cmd := exec.CommandContext(ctx, npm, "run", "build")
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build failed %v", err)
	}
	bundled := fmt.Sprintf("dist/%s.js", cfg.Name)
	b, err := os.ReadFile(bundled)
	if err != nil {
		return fmt.Errorf("unable to read %q: %v", bundled, err)
	}
	watFile := createWatFromJavaScript(b)
	if err := afero.WriteFile(fs, "dist/source.wat", []byte(watFile), os.FileMode(0o644)); err != nil {
		return fmt.Errorf("unable to write %q: %v", "dist/source.wat", err)
	}
	if _, err = buildpack.JavaScript.Install(ctx, fs); err != nil {
		return err
	}
	bpRoot, err := buildpack.JavaScript.RootPath()
	if err != nil {
		return err
	}
	wasmMerge := path.Join(bpRoot, "bin", "wasm-merge")
	jsVMWasm := path.Join(bpRoot, "bin", "redpanda_js_transform")
	cmd = exec.CommandContext(
		ctx,
		wasmMerge,
		jsVMWasm,
		"js_vm",
		"dist/source.wat",
		"redpanda_js_provider",
		"-mvp",
		"--enable-simd",
		"--enable-bulk-memory",
		"--enable-multimemory",
		"-o",
		fmt.Sprintf("%s.wasm", cfg.Name),
	)
	cmd.Stderr = os.Stderr
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("build failed %v", err)
	}
	return nil
}
