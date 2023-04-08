// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package wasm

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/wasm/template"
	vos "github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func newGenerateCommand(fs afero.Fs) *cobra.Command {
	var skipVersion bool
	cmd := &cobra.Command{
		Use:   "generate [PROJECT DIRECTORY]",
		Short: "Create a npm template project for inline WASM engine",
		Args:  cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			path, err := filepath.Abs(args[0])
			out.MaybeDie(err, "unable to get absolute path for %q: %v", args[0], err)
			err = executeGenerate(fs, path, skipVersion)
			out.MaybeDie(err, "unable to generate all manifest files: %v", err)
		},
	}
	cmd.Flags().BoolVar(&skipVersion, "skip-version", false, "Omit wasm-api version check from npm, use default instead")
	return cmd
}

type genFile struct {
	name       string
	content    string
	permission os.FileMode
}

func generateManifest(version string) map[string][]genFile {
	return map[string][]genFile{
		"src":  {genFile{name: "main.js", content: template.WasmJs()}},
		"test": {genFile{name: "main.test.js", content: template.WasmTestJs()}},
		"": {
			genFile{name: "package.json", content: template.PackageJSON(version)},
			genFile{name: "webpack.js", content: template.Webpack(), permission: 0o766},
		},
	}
}

const defAPIVersion = "21.8.2"

func getWasmAPIVersion(wasmAPI string) string {
	var result []map[string]interface{}
	if err := json.Unmarshal([]byte(wasmAPI), &result); err != nil {
		fmt.Printf("Can not parse json from npm search: '%s', Error: %s\n", wasmAPI, err)
		return defAPIVersion
	}

	if len(result) != 1 {
		fmt.Printf("Wrong npm search result: %v", result)
		return defAPIVersion
	}

	version, ok := result[0]["version"].(string)
	if !ok {
		fmt.Printf("Can not get version from npm search result: %s\n", result)
		return defAPIVersion
	}
	return version
}

// latestClientApiVersion looks up the latest version of our client library using npm,
// defaulting if anything fails.
func latestClientAPIVersion() string {
	if _, err := exec.LookPath("npm"); err != nil {
		fmt.Printf("npm not found, defaulting to client API version %s.\n", defAPIVersion)
		return defAPIVersion
	}

	proc := vos.NewProc()
	output, err := proc.RunWithSystemLdPath(2*time.Second, "npm", "search", "@redpanda-data/wasm-api", "--json")
	if err != nil {
		zap.L().Sugar().Error(err)
		return defAPIVersion
	}

	wasmAPI := strings.Join(output, "")

	return getWasmAPIVersion(wasmAPI)
}

func executeGenerate(fs afero.Fs, path string, skipVersion bool) error {
	var preexisting []string
	var version string
	if skipVersion {
		version = defAPIVersion
	} else {
		version = latestClientAPIVersion()
	}
	for dir, templates := range generateManifest(version) {
		for _, template := range templates {
			file := filepath.Join(path, dir, template.name)
			exist, err := afero.Exists(fs, file)
			if err != nil {
				return fmt.Errorf("unable to determine if file %q exists: %v", file, err)
			}
			if exist {
				preexisting = append(preexisting, file)
			}
		}
	}
	if len(preexisting) > 0 {
		return fmt.Errorf("files already exist; try using a new directory or removing the existing files, existing: %v", preexisting)
	}

	if err := fs.MkdirAll(path, 0o755); err != nil {
		return err
	}
	for dir, templates := range generateManifest(version) {
		dirPath := filepath.Join(path, dir)
		if err := fs.MkdirAll(dirPath, 0o755); err != nil {
			return err
		}
		for _, template := range templates {
			file := filepath.Join(dirPath, template.name)
			perm := os.FileMode(0o600)
			if template.permission > 0 {
				perm = template.permission
			}
			if err := afero.WriteFile(fs, file, []byte(template.content), perm); err != nil {
				return err
			}
		}
	}
	fmt.Printf("npm created project in %s\n", path)
	return nil
}
