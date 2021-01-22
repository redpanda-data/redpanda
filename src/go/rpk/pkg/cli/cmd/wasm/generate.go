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

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/cli/cmd/wasm/template"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/utils"
)

type genFile struct {
	name		string
	content		string
	permission	os.FileMode
}

var manifest = func() map[string][]genFile {
	return map[string][]genFile{
		"src":	{genFile{name: "wasm.js", content: template.GetWasmJs()}},
		"test":	{genFile{name: "wasm.test.js", content: template.GetWasmTestJs()}},
		"": {
			genFile{name: "package.json", content: template.GetPackageJson()},
			genFile{name: "webpack.js", content: template.GetWebpack(), permission: 0766},
		},
	}
}

func NewGenerateCommand(fs afero.Fs) *cobra.Command {
	var dir string
	const localDir = "./"
	command := &cobra.Command{
		Use:	"generate",
		Short:	"Create a npm template project for inline WASM engine",
		RunE: func(_ *cobra.Command, args []string) error {
			if dir == localDir {
				path, err := filepath.Abs(dir)
				if err != nil {
					return err
				}
				rootPath := filepath.Join(path, "wasm")
				return executeGenerate(fs, rootPath)

			}
			path, err := filepath.Abs(dir)
			if err != nil {
				return err
			}
			return executeGenerate(fs, path)
		},
	}

	command.Flags().StringVar(
		&dir,
		"dir",
		localDir,
		"The path where the transformer project will be created",
	)

	return command
}

func createIfNotExist(fs afero.Fs, path string) (afero.File, error) {
	exist, err := afero.Exists(fs, path)
	if err != nil {
		return nil, err
	}
	if exist {
		folderPath, filePath := filepath.Split(path)
		return nil, fmt.Errorf("The directory %s contains files that could conflict: \n %s", folderPath, filePath)
	}
	return fs.Create(path)
}

func executeGenerate(fs afero.Fs, path string) error {
	err := fs.MkdirAll(path, 0755)
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
	}
	for folderName, templates := range manifest() {
		folderPath := filepath.Join(path, folderName)
		err := fs.MkdirAll(folderPath, 0755)
		if err != nil && !os.IsExist(err) {
			return err
		}
		for _, templateFile := range templates {
			filePath := filepath.Join(folderPath, templateFile.name)
			_, err := createIfNotExist(fs, filePath)
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
