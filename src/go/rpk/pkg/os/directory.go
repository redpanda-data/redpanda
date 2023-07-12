// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package os

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/afero"
)

// PrintDirectoryTree prints the list of contents of the root directory in a
// tree-like format.
func PrintDirectoryTree(fs afero.Fs, root string, indent string) error {
	fileInfos, err := afero.ReadDir(fs, root)
	if err != nil {
		return err
	}
	var directories, files []os.FileInfo
	for _, fileInfo := range fileInfos {
		if fileInfo.IsDir() {
			directories = append(directories, fileInfo)
		} else {
			files = append(files, fileInfo)
		}
	}

	for i, directory := range directories {
		isLast := i == len(directories)-1 && len(files) == 0

		fmt.Printf("%s", indent)
		if isLast {
			fmt.Print("└── ")
		} else {
			fmt.Print("├── ")
		}
		fmt.Println(directory.Name())

		if !isLast {
			newIndent := indent + "│   "
			err = PrintDirectoryTree(fs, filepath.Join(root, directory.Name()), newIndent)
			if err != nil {
				return err
			}
		}
	}

	for i, file := range files {
		isLast := i == len(files)-1

		fmt.Printf("%s", indent)
		if isLast {
			fmt.Print("└── ")
		} else {
			fmt.Print("├── ")
		}
		fmt.Println(file.Name())
	}
	return nil
}
