/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package template

import (
	_ "embed"
	"encoding/json"
	"fmt"
)

type packageJSON struct {
	Name            string            `json:"name"`
	Type            string            `json:"type"`
	Private         bool              `json:"private"`
	Scripts         map[string]string `json:"scripts"`
	Dependencies    map[string]string `json:"dependencies"`
	DevDependencies map[string]string `json:"devDependencies"`
}

func WasmPackageJSON(name string, typescript bool) string {
	pkg := packageJSON{
		Name:    name,
		Private: true,
		Type:    "module",
		Scripts: map[string]string{
			"build": "node esbuild.js",
		},
		Dependencies: map[string]string{
			"@redpanda-data/transform-sdk": "1.x",
		},
		DevDependencies: map[string]string{
			// Our (excellent) bundler
			"esbuild": "0.20.x",
			// A plugin for node compat
			"esbuild-plugin-polyfill-node": "0.3.x",
		},
	}
	if typescript {
		// Add typescript compiled if needed
		pkg.DevDependencies["typescript"] = "5.x"
	}
	b, _ := json.MarshalIndent(&pkg, "", "  ")
	return string(b)
}

//go:embed js/README.md
var wasmJsReadme string

func WasmJavaScriptReadme(typescript bool) string {
	lang := "JavaScript"
	ext := "js"
	if typescript {
		lang = "TypeScript"
		ext = "ts"
	}
	return fmt.Sprintf(wasmJsReadme, lang, ext)
}

func WasmTypeScriptReadme() string {
	return wasmJsReadme
}

//go:embed js/tsconfig.json
var wasmTsConfig string

func WasmTsConfig() string {
	return wasmTsConfig
}

//go:embed js/transform.jssrc
var wasmJsMain string

func WasmJavaScriptMain() string {
	return wasmJsMain
}

//go:embed js/transform.tssrc
var wasmTsMain string

func WasmTypeScriptMain() string {
	return wasmTsMain
}

//go:embed js/esbuild.js
var wasmEsbuildFile string

func WasmEsbuildFile(name string, typescript bool) string {
	ext := ".js"
	if typescript {
		ext = ".ts"
	}
	return fmt.Sprintf(wasmEsbuildFile, ext, name+".js")
}
