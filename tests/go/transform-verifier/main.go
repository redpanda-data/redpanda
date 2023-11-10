/*
* Copyright 2023 Redpanda Data, Inc.
*
* Use of this software is governed by the Business Source License
* included in the file licenses/BSL.md
*
* As of the Change Date specified in that file, in accordance with
* the Business Source License, use of this software will be governed
* by the Apache License, Version 2.0
 */

package main

import (
	"os"
	"sync"

	"github.com/spf13/cobra"
	"log/slog"
	"redpanda.com/testing/transform-verifier/common"
	"redpanda.com/testing/transform-verifier/consume"
	"redpanda.com/testing/transform-verifier/produce"
)

var (
	port int16

	serverDoneWg = &sync.WaitGroup{}
	rootCmd      = &cobra.Command{
		Use:   "transform-verifier",
		Short: "A verifier for data transforms",
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			slog.Debug("initializing status")
			status := common.NewStatusReporter(port)
			common.SetStatusReporter(cmd.Context(), status)
			serverDoneWg.Add(1)
			go func() {
				defer serverDoneWg.Done()
				slog.Debug("starting http status server")
				err := status.ServeHTTP(cmd.Context())
				if err != nil {
					common.Die("error serving HTTP: %v", err)
				}
				slog.Debug("stopped http status server")
			}()
			slog.Debug("status initialized")
		},
		PersistentPostRun: func(cmd *cobra.Command, args []string) {
			slog.Debug("waiting for server to exit")
			// Wait for the server to be stopped to exit, this is done via
			// a server kill request so we give time for the testing
			// infrastructure to get the latest status.
			serverDoneWg.Wait()
			slog.Debug("server shutdown complete")
		},
	}
)

func init() {
	rootCmd.PersistentFlags().Int16Var(&port, "port", 8080, "port for http status server")
	common.AddFranzGoFlags(rootCmd)

	rootCmd.AddCommand(
		produce.NewCommand(),
		consume.NewCommand(),
	)
}

func main() {
	if err := rootCmd.ExecuteContext(common.MakeRootContext()); err != nil {
		os.Exit(1)
	}
	slog.Info("shutdown complete")
}
