// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package wasm

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/wasm/template"
	vos "github.com/vectorizedio/redpanda/src/go/rpk/pkg/os"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/utils"
)

type genFile struct {
	name       string
	content    string
	permission os.FileMode
}

func GenerateManifest(version string) map[string][]genFile {
	return map[string][]genFile{
		"src":  {genFile{name: "main.js", content: template.GetWasmJs()}},
		"test": {genFile{name: "main.test.js", content: template.GetWasmTestJs()}},
		"": {
			genFile{name: "package.json", content: fmt.Sprintf(template.GetPackageJson(), version)},
			genFile{name: "webpack.js", content: template.GetWebpack(), permission: 0766},
		},
	}
}

func NewGenerateCommand(fs afero.Fs) *cobra.Command {
	command := &cobra.Command{
		Use:          "generate <project directory>",
		Short:        "Create an npm template project for inline WASM engine",
		SilenceUsage: true,
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return fmt.Errorf(
					"no project directory specified",
				)
			}
			return nil
		},
		RunE: func(_ *cobra.Command, args []string) error {
			path, err := filepath.Abs(args[0])
			if err != nil {
				return err
			}
			return executeGenerate(fs, path)
		},
	}

	return command
}

func checkIfExists(fs afero.Fs, path string) error {
	exist, err := afero.Exists(fs, path)
	if err != nil {
		return err
	}
	if exist {
		folderPath, filePath := filepath.Split(path)
		return fmt.Errorf("The directory %s contains files that could conflict: \n %s", folderPath, filePath)
	}
	return nil
}

func GetLatestClientApiVersion() string {
	/// Works by using npm to lookup the latest version of our library
	timeout := 2 * time.Second
	version := "21.8.2"
	proc := vos.NewProc()
	output, err := proc.RunWithSystemLdPath(timeout, "npm", "search", "@vectorizedio/wasm-api")
	if err != nil {
		log.Error(err)
		return version
	}
	for line := range output {
		resultLine := output[line]
		splitResults := strings.Split(resultLine, "|")
		if strings.TrimSpace(splitResults[0]) == "@vectorizedio/wasm-api" {
			version = strings.TrimSpace(splitResults[len(splitResults)-2])
			break
		}
	}
	return version
}

func executeGenerate(fs afero.Fs, path string) error {
	err := fs.MkdirAll(path, 0755)
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	for folderName, templates := range GenerateManifest(GetLatestClientApiVersion()) {
		folderPath := filepath.Join(path, folderName)
		err := fs.MkdirAll(folderPath, 0755)
		if err != nil && !os.IsExist(err) {
			return err
		}
		for _, templateFile := range templates {
			filePath := filepath.Join(folderPath, templateFile.name)
			err := checkIfExists(fs, filePath)
			if err != nil {
				return err
			}
			_, err = utils.WriteBytes(fs, []byte(templateFile.content), filePath)
			if err != nil {
				return err
			}

			if templateFile.permission > 0 {
				err = fs.Chmod(filePath, templateFile.permission)
				if err != nil {
					return err
				}
			}
		}
	}
	log.Infof("npm created project in %s", path)
	return nil
}
