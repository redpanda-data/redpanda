// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package embed

import (
	"embed"
	"io/fs"
	"path/filepath"
	"sync"
)

//go:embed protobuf/google/type/*.proto protobuf/confluent/*.proto protobuf/confluent/types/*.proto
var content embed.FS

var (
	once     sync.Once
	protoMap map[string]string
)

// CommonProtoFiles returns the file system representation of the common
// protobuf types.
func CommonProtoFiles() (fs.FS, error) {
	return fs.Sub(content, "protobuf")
}

// CommonProtoFileMap returns the map representation of the common protobuf
// types. This is useful for protoreflect parsing.
func CommonProtoFileMap() (map[string]string, error) {
	protoFS, err := CommonProtoFiles()
	if err != nil {
		return nil, err
	}

	once.Do(func() {
		protoMap = make(map[string]string)
		err = fs.WalkDir(protoFS, ".", func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || filepath.Ext(path) != ".proto" {
				return nil
			}
			data, err := fs.ReadFile(protoFS, path)
			if err == nil {
				protoMap[path] = string(data)
			}
			return nil
		})
	})

	if err != nil {
		return nil, err
	}
	return protoMap, err
}
