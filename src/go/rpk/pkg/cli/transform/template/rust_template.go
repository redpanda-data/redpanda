// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package template

import (
	_ "embed"
	"fmt"
)

//go:embed rust/main.rustsrc
var rustMainFile string

func WasmRustMain() string {
	return rustMainFile
}

const cargoTomlTemplate = `[package]
name = "%s"
version = "0.1.0"
edition = "2021"

[dependencies]
`

func WasmRustCargoConfig(name string) string {
	return fmt.Sprintf(cargoTomlTemplate, name)
}

//go:embed rust/README.md
var wasmRustReadme string

func WasmRustReadme() string {
	return wasmRustReadme
}
