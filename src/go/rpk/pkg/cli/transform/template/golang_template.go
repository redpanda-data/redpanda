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

//go:embed golang/transform.gosrc
var wasmMainFile string

func WasmGoMain() string {
	return wasmMainFile
}

const wasmGoModFile = `module %s

go 1.20
`

func WasmGoModule(name string) string {
	return fmt.Sprintf(wasmGoModFile, name)
}

//go:embed golang/README.md
var wasmGoReadme string

func WasmGoReadme() string {
	return wasmGoReadme
}
