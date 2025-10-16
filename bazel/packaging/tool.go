// Copyright 2025 Redpanda Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing/fstest"
	"time"
)

type fileConfig struct {
	Path       string `json:"path"`
	Name       string `json:"name"`
	SourcePath string `json:"source"`
}

type pkgConfig struct {
	PackageDirectories []string     `json:"package_dirs"`
	PackageFiles       []fileConfig `json:"package_files"`
	DirectoryMode      bool         `json:"directory_mode"`
	Owner              int          `json:"owner"`
}

const rootDir = "___root___"

// buildPackageStructure creates a MapFS structure based on the provided package configuration.
// It sets up directories and package files specified in the configuration.
// The root directory is defined as "___root___" and all paths are relative to this root.
// NOTE: the root directory is required to be present in the MapFS structure for the walkDir function to work.

func buildPackageStructure(cfg pkgConfig) (fstest.MapFS, error) {
	mapFs := fstest.MapFS{}

	// Add directories
	for _, dir := range cfg.PackageDirectories {
		path := filepath.Join(rootDir, dir)
		mapFs[path] = &fstest.MapFile{
			Mode: fs.ModeDir,
		}
	}

	// Add package files
	for _, file := range cfg.PackageFiles {
		path := filepath.Join(rootDir, file.Path, file.Name)
		mapFs[path] = &fstest.MapFile{}
	}

	return mapFs, nil
}

func createPackageDirectories(mapFs fstest.MapFS, dirFunction func(path string) error) error {
	return fs.WalkDir(mapFs, rootDir, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if path == rootDir {
			return nil // Skip the root directory itself
		}
		// Get the relative path from the root directory, we do not want the root directory in the path
		relativePath, err := filepath.Rel(rootDir, path)

		if err != nil {
			return err
		}
		if info.IsDir() {
			return dirFunction(relativePath)
		}
		return nil
	})
}

func createPackage(cfg pkgConfig, createDir func(path string) error, createFile func(fileConfig fileConfig) error) error {
	// Create package directory structure
	mapFs, err := buildPackageStructure(cfg)
	if err != nil {
		return fmt.Errorf("unable to build package structure: %w", err)
	}

	err = createPackageDirectories(mapFs, createDir)
	if err != nil {
		return fmt.Errorf("error creating package directories: %w", err)
	}

	for _, f := range cfg.PackageFiles {
		if err := createFile(f); err != nil {
			return fmt.Errorf("error creating package file %s: %w", f.SourcePath, err)
		}
	}

	return nil
}

func createTarball(cfg pkgConfig, w io.Writer) error {
	tw := tar.NewWriter(w)
	defer func() {
		if err := tw.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "error closing tar writer: %v\n", err)
		}
	}()
	writeFile := func(fileConfig fileConfig) error {
		file, err := os.Open(fileConfig.SourcePath)
		if err != nil {
			return err
		}
		defer func() {
			if err := file.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "error closing file %s: %v\n", fileConfig.SourcePath, err)
			}
		}()
		info, err := file.Stat()
		if err != nil {
			return err
		}
		err = tw.WriteHeader(&tar.Header{
			Name:     filepath.Join(fileConfig.Path, fileConfig.Name),
			Mode:     int64(info.Mode()),
			Typeflag: tar.TypeReg,
			ModTime:  time.Unix(0, 0),
			Uid:      cfg.Owner,
			Gid:      cfg.Owner,
			Size:     info.Size(),
		})
		if err != nil {
			return err
		}
		_, err = io.Copy(tw, file)
		return err
	}
	writeDir := func(path string) error {
		return tw.WriteHeader(&tar.Header{
			Name:     path,
			Mode:     0755,
			Typeflag: tar.TypeDir,
			ModTime:  time.Unix(0, 0),
			Uid:      cfg.Owner,
			Gid:      cfg.Owner,
		})
	}
	return createPackage(cfg, writeDir, writeFile)
}

func copyFile(src string, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		if err := srcFile.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "error closing source file %s: %v\n", src, err)
		}
	}()
	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		if err := dstFile.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "error closing destination file %s: %v\n", dst, err)
		}
	}()
	_, err = dstFile.ReadFrom(srcFile)
	if err != nil {
		return err
	}

	return nil
}

func createPackageDir(cfg pkgConfig, output string) error {
	if err := os.MkdirAll(output, 0755); err != nil {
		return err
	}

	dir := func(path string) error {
		if err := os.Mkdir(filepath.Join(output, path), 0755); err != nil {
			return fmt.Errorf("error creating directory %s: %v", path, err)
		}
		return nil
	}

	file := func(fileConfig fileConfig) error {
		if err := copyFile(fileConfig.SourcePath, filepath.Join(output, fileConfig.Path, fileConfig.Name)); err != nil {
			return fmt.Errorf("error copying file %s: %v", fileConfig.SourcePath, err)
		}
		return nil
	}

	return createPackage(cfg, dir, file)
}

func runTool() error {
	configPath := flag.String("config", "", "path to a configuration file to create the tarball")
	output := flag.String("output", "redpanda.tar.gz", "the output .tar.gz location")
	flag.Parse()
	var cfg pkgConfig
	if b, err := os.ReadFile(*configPath); err != nil {
		return fmt.Errorf("unable to read file: %w", err)
	} else if err := json.Unmarshal(b, &cfg); err != nil {
		return fmt.Errorf("unable to parse config: %w", err)
	}

	if cfg.DirectoryMode {
		if err := createPackageDir(cfg, *output); err != nil {
			return fmt.Errorf("unable to create package directory: %w", err)
		}
	} else {
		file, err := os.OpenFile(*output, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
		if err != nil {
			return fmt.Errorf("unable to open output file: %w", err)
		}
		defer func() {
			if err := file.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "error closing output file %s: %v\n", *output, err)
			}
		}()
		bw := bufio.NewWriter(file)
		defer func() {
			if err := bw.Flush(); err != nil {
				fmt.Fprintf(os.Stderr, "error flushing buffered writer: %v\n", err)
			}
		}()
		gw := gzip.NewWriter(bw)
		defer func() {
			if err := gw.Close(); err != nil {
				fmt.Fprintf(os.Stderr, "error closing gzip writer: %v\n", err)
			}
		}()
		if err := createTarball(cfg, gw); err != nil {
			return fmt.Errorf("unable to create tarball: %w", err)
		}
	}
	return nil
}

func main() {
	if err := runTool(); err != nil {
		fmt.Fprintf(os.Stderr, "unable to generate package: %s", err.Error())
		os.Exit(1)
	}
}
