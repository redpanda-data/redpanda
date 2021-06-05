// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/fatih/color"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"golang.org/x/crypto/ssh/terminal"
)

func Execute() {
	verbose := false
	fs := afero.NewOsFs()
	mgr := config.NewManager(fs)

	if !terminal.IsTerminal(int(os.Stdout.Fd())) {
		color.NoColor = true
	}
	log.SetFormatter(cli.NewRpkLogFormatter())
	log.SetOutput(os.Stdout)

	cobra.OnInitialize(func() {
		// This is only executed when a subcommand (e.g. rpk check) is
		// specified.
		if verbose {
			log.SetLevel(log.DebugLevel)
			// Make sure we enable verbose logging for sarama client
			// we configure the Sarama logger only for verbose output as sarama
			// logger use no severities. It is either enabled or disabled.
			sarama.Logger = &log.Logger{
				Out:          os.Stderr,
				Formatter:    cli.NewRpkLogFormatter(),
				Hooks:        make(log.LevelHooks),
				Level:        log.DebugLevel,
				ExitFunc:     os.Exit,
				ReportCaller: false,
			}
		} else {
			log.SetLevel(log.InfoLevel)
		}
	})

	rootCmd := &cobra.Command{
		Use:   "rpk",
		Short: "rpk is the Redpanda CLI & toolbox",
		Long:  "",
	}
	rootCmd.SilenceUsage = true
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose",
		"v", false, "enable verbose logging (default false)")

	rootCmd.AddCommand(NewModeCommand(mgr))
	rootCmd.AddCommand(NewGenerateCommand(mgr))
	rootCmd.AddCommand(NewVersionCommand())
	rootCmd.AddCommand(NewWasmCommand(fs, mgr))
	rootCmd.AddCommand(NewContainerCommand())
	rootCmd.AddCommand(NewTopicCommand(fs, mgr))
	rootCmd.AddCommand(NewClusterCommand(fs, mgr))
	rootCmd.AddCommand(NewCloudCommand(fs))
	rootCmd.AddCommand(NewACLCommand(fs, mgr))

	addPlatformDependentCmds(fs, mgr, rootCmd)

	// Plugins: we favor rpk commands first, but if we cannot find one, we
	// try a plugin. If we exec a plugin, tryExecPlugin exits.
	if _, _, err := rootCmd.Find(os.Args[1:]); err != nil {
		if foundPath, err := tryExecPlugin(new(osPluginHandler), os.Args[1:]); len(foundPath) > 0 {
			if err != nil {
				log.Fatalf("exec %s: %v\n", foundPath, err)
			}
			os.Exit(0)
		}
	}

	err := rootCmd.Execute()
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "check":
			fallthrough
		case "tune":
			log.Info(common.FeedbackMsg)
		}
	}
	if err != nil {
		os.Exit(1)
	}
}

type pluginHandler interface {
	lookPath(file string) (path string, ok bool)
	exec(path string, args []string) error
}

// tryExecPlugin looks for a plugin, following the exact behavior of kubernetes:
//
//  - all dashes are turned to underscores
//  - all "pieces" (non-flags) are joined with a dash
//  - we prefer the longest command match
//  - we search upward by piece until we run out of pieces
//  - lastly, the command must be executable
//
// Dashes are only replaced into underscores during the path search; if an
// argument hash a dash and is used as an argument to a plugin, the dash
// remains.
//
// So,
//
//     rpk foo-bar baz boz fizz-buzz --flag
//
// is translated into searching and execing (with osPluginHandler), in order:
//
//     rpk-foo_bar-baz-boz-fizz_buzz (with args "--flag")
//     rpk-foo_bar-baz-boz           (with args "fizz-buzz --flag")
//     rpk-foo_bar-baz               (with args "boz fizz-buzz --flag")
//     rpk-foo_bar                   (with args "baz boz fizz-buzz --flag")
//
// If a plugin is run, this returns the run error and true, otherwise this
// returns false.
func tryExecPlugin(h pluginHandler, args []string) (string, error) {
	var pieces []string
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") { // found a flag, quit
			break
		}
		pieces = append(pieces, strings.ReplaceAll(arg, "-", "_"))
	}

	if len(pieces) == 0 {
		return "", nil // no plugin specified (command is just "rpk")
	}

	foundPath := ""
	for len(pieces) > 0 {
		path, ok := h.lookPath("rpk-" + strings.Join(pieces, "-"))
		if !ok { // did not find with this piece, strip and search higher
			pieces = pieces[:len(pieces)-1]
			continue
		}
		foundPath = path
		break
	}
	if len(foundPath) == 0 {
		return "", nil
	}

	return foundPath, h.exec(foundPath, args[len(pieces):])
}

type osPluginHandler struct{}

func (*osPluginHandler) lookPath(file string) (string, bool) {
	path, err := exec.LookPath(file)
	return path, err == nil
}
func (*osPluginHandler) exec(path string, args []string) error {
	args = append([]string{path}, args...)
	env := os.Environ()
	if runtime.GOOS == "windows" {
		return (&exec.Cmd{
			Path:   path,
			Args:   args,
			Env:    env,
			Stdin:  os.Stdin,
			Stdout: os.Stdout,
			Stderr: os.Stderr,
		}).Run()
	}
	return syscall.Exec(path, args, env)
}
