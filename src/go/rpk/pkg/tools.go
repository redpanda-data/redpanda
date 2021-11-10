//go:build tools
// +build tools

package main

import (
	_ "github.com/cockroachdb/crlfmt"
	_ "mvdan.cc/sh/v3/cmd/shfmt"
)
