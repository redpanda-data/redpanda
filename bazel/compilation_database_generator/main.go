/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

// This is a golang program to generate a compile_commands.json for clangd
// and other C++ tooling.
//
// It is mostly a simplified port of https://github.com/hedronvision/bazel-compile-commands-extractor
// as that contains a bunch of stuff for other platforms or compilers that we don't care about.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
)

// SwitchCWDToWorkspaceRoot switches CWD to the Bazel workspace root
func SwitchCWDToWorkspaceRoot() error {
	dir, ok := os.LookupEnv("BUILD_WORKSPACE_DIRECTORY")
	if !ok {
		return errors.New("BUILD_WORKSPACE_DIRECTORY was not found in the environment. Make sure to invoke this with `bazel run`")
	}
	if err := os.Chdir(dir); err != nil {
		return fmt.Errorf("unable to change working directory to workspace root: %w", err)
	}
	return nil
}

func SymlinkExternalToWorkspaceRoot() error {
	if _, err := os.Lstat("bazel-out"); errors.Is(err, os.ErrNotExist) {
		return errors.New("//bazel-out is missing!")
	}
	// Traverse into output_base via bazel-out, keeping the workspace position-independent, so it can be moved without rerunning
	src := "external"
	dest := "bazel-out/../../../external"
	if _, err := os.Lstat(src); err == nil || !errors.Is(err, os.ErrNotExist) {
		currentDest, err := os.Readlink(src)
		if err != nil {
			return fmt.Errorf("unable to resolve external directory symlink: %w", err)
		}
		if dest != currentDest {
			fmt.Fprintln(os.Stderr, "//external links to the wrong place. Automatically deleting and relinking...")
			if err := os.Remove(src); err != nil {
				return fmt.Errorf("unable to cleanup invalid external symlink: %w", err)
			}
		}
	}
	if _, err := os.Lstat(src); errors.Is(err, os.ErrNotExist) {
		if err := os.Symlink(dest, src); err != nil {
			return fmt.Errorf("unable to create external symlink: %w", err)
		}
		fmt.Println("Automatically added //external workspace link:")
		fmt.Println("This link makes it easy for you--and for build tooling--to see the external dependencies you bring in. It also makes your source tree have the same directory structure as the build sandbox.")
		fmt.Println("It's a win/win: It's easier for you to browse the code you use, and it eliminates whole categories of edge cases for build tooling.")
	}
	return nil
}

type (
	KeyValue struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	AqueryAction struct {
		TargetID    int        `json:"targetId"`
		Arguments   []string   `json:"arguments"`
		Environment []KeyValue `json:"environmentVariables"`
	}
	AqueryTarget struct {
		ID    int    `json:"id"`
		Label string `json:"label"`
	}
	AqueryOutput struct {
		Actions []AqueryAction `json:"actions"`
		Targets []AqueryTarget `json:"targets"`
	}
)

// CompileCommand is a single entry in the compile_commands.json file.
// Docs about compile_commands.json format: https://clang.llvm.org/docs/JSONCompilationDatabase.html#format
type CompileCommand struct {
	File      string   `json:"file"`
	Arguments []string `json:"arguments"`
	// Bazel gotcha warning: If you were tempted to use `bazel info execution_root` as the build working directory for compile_commands...search ImplementationReadme.md in the Hedron repo to learn why that breaks.
	Directory string `json:"directory"`
}

// ConvertCompileCommands converts from Bazel's aquery format to de-Bazeled compile_commands.json entries.
func GetCppCommandForFiles(action AqueryAction) (src string, args []string, err error) {
	if len(action.Arguments) == 0 {
		err = errors.New("empty arguments for compiler action")
		return
	}
	args = make([]string, 0, len(action.Arguments)+2)
	// TODO(bazel): We might need to preprocess the compiler location if it's a llvm_toolchain one
	// because in the current form, the compiler headers are in the wrong place.
	args = append(args, action.Arguments[0])
	for i, curr := range action.Arguments[1:] {
		prev := action.Arguments[i]
		// NOTE: if additionally add src/v as a search path *first* so that we
		// don't find virtual included symlink'd headers by default for our source
		// code. This is a slight hack, but does mirror what we used to do in CMake.
		// Bazel itself has this repo root include that a normal "bazel" project would
		// use. Bazel enforces only the right headers are available at build time via
		// the sandbox anyways. This prevents jumping to a sandbox location for one of
		// the headers in the redpanda repo.
		if curr == "." && prev == "-iquote" {
			args = append(args, "src/v", "-iquote")
		}
		// We're transfering the commands as though they were compiled in place in the workspace; no need for prefix maps, so we'll remove them. This eliminates some postentially confusing Bazel variables, though I think clangd just ignores them anyway.
		// Some example:
		// -fdebug-prefix-map=__BAZEL_EXECUTION_ROOT__=.
		if strings.HasPrefix(curr, "-fdebug-prefix-map") {
			continue
		}
		if prev == "-c" {
			src = curr
		}
		args = append(args, curr)
	}
	if src == "" {
		err = fmt.Errorf("unable to find source .cc file for targetId %d", action.TargetID)
		return
	}
	return
}

// ConvertCompileCommands converts from Bazel's aquery format to de-Bazeled compile_commands.json entries.
func ConvertCompileCommands(output AqueryOutput) ([]CompileCommand, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return nil, fmt.Errorf("unable to get cwd: %w", err)
	}
	cmds := make([]CompileCommand, 0, len(output.Actions))
	for _, action := range output.Actions {
		src, args, err := GetCppCommandForFiles(action)
		if err != nil {
			return nil, fmt.Errorf("unable to get cpp command: %w", err)
		}
		// Skip Bazel internal files
		if strings.HasPrefix(src, "external/bazel_tools/") {
			continue
		}
		cmds = append(cmds, CompileCommand{
			File:      src,
			Arguments: args,
			Directory: cwd,
		})
	}
	return cmds, nil
}

// GetCommands yields compile_commands.json entries
func GetCommands(extraArgs ...string) ([]CompileCommand, error) {
	cmd := exec.Command(
		"bazel",
		"aquery",
		// Aquery docs if you need em: https://docs.bazel.build/versions/master/aquery.html
		// Aquery output proto reference: https://github.com/bazelbuild/bazel/blob/master/src/main/protobuf/analysis_v2.proto
		// One bummer, not described in the docs, is that aquery filters over *all* actions for a given target,
		// rather than just those that would be run by a build to produce a given output.
		// This mostly isn't a problem, but can sometimes surface extra, unnecessary, misconfigured actions.
		// See: https://github.com/bazelbuild/bazel/issues/14156
		`mnemonic('CppCompile', filter('^(//|@//)', deps(//...)))`,
		// We switched to jsonproto instead of proto because of https://github.com/bazelbuild/bazel/issues/13404.
		// We could change back when fixed--reverting most of the commit that added this line and tweaking the
		// build file to depend on the target in that issue. That said, it's kinda nice to be free of the dependency,
		// unless (OPTIMNOTE) jsonproto becomes a performance bottleneck compated to binary protos.
		"--output=jsonproto",
		// We'll disable artifact output for efficiency, since it's large and we don't use them.
		// Small win timewise, but dramatically less json output from aquery.
		"--include_artifacts=false",
		// Shush logging. Just for readability.
		"--ui_event_filters=-info",
		"--noshow_progress",
		// Disable param files, which would obscure compile actions
		// Mostly, people enable param files on Windows to avoid the relatively short command length limit.
		// For more, see compiler_param_file in https://bazel.build/docs/windows
		// They are, however, technically supported on other platforms/compilers.
		// That's all well and good, but param files would prevent us from seeing compile actions before the
		// param files had been generated by compilation.
		// Since clangd has no such length limit, we'll disable param files for our aquery run.
		"--features=-compiler_param_file",
		"--host_features=-compiler_param_file",
		// Disable layering_check during, because it causes large-scale dependence on generated module map files
		// that prevent header extraction before their generation
		// For more context, see https://github.com/hedronvision/bazel-compile-commands-extractor/issues/83
		// If https://github.com/clangd/clangd/issues/123 is resolved and we're not doing header extraction, we
		// could try removing this, checking that there aren't erroneous red squigglies squigglies before the module maps are generated.
		// If Bazel starts supporting modules (https://github.com/bazelbuild/bazel/issues/4005), we'll probably
		// need to make changes that subsume this.
		"--features=-layering_check",
		"--host_features=-layering_check",
		// Disable parse_headers features, this causes some issues with generating compilation actions with no source files.
		// See: https://github.com/hedronvision/bazel-compile-commands-extractor/issues/211
		"--features=-parse_headers",
		"--host_features=-parse_headers",
	)
	cmd.Args = append(cmd.Args, extraArgs...)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("unable to connect to `bazel aquery` stdout: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("unable to run `bazel aquery`: %w", err)
	}
	stdout, stdoutErr := io.ReadAll(stdoutPipe)
	if err := cmd.Wait(); err != nil {
		return nil, fmt.Errorf("unable to run `bazel aquery`: %w", err)
	}
	if stdoutErr != nil {
		return nil, fmt.Errorf("unable to get `bazel aquery` stdout: %w", stdoutErr)
	}
	var output AqueryOutput
	if err := json.Unmarshal(stdout, &output); err != nil {
		return nil, fmt.Errorf("unable to parse `bazel aquery` stdout: %w", err)
	}
	if len(output.Actions) == 0 {
		return nil, errors.New("unable to find any actions from `bazel aquery`, likely there are BUILD file errors")
	}
	return ConvertCompileCommands(output)
}

func RunTool(args []string) error {
	if err := SwitchCWDToWorkspaceRoot(); err != nil {
		return err
	}
	if err := SymlinkExternalToWorkspaceRoot(); err != nil {
		return err
	}
	cmds, err := GetCommands(args...)
	if err != nil {
		return err
	}
	buf, err := json.MarshalIndent(cmds, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile("compile_commands.json", buf, 0o664)
}

func main() {
	if err := RunTool(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "unable to generate compilation database: %s", err.Error())
		os.Exit(1)
	}
}
