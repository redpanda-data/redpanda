// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cli

import (
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/version"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type flagPrint struct {
	Name        string `json:"name"`
	Shorthand   string `json:"shorthand,omitempty"`
	Type        string `json:"type"`
	Description string `json:"description"`
	Default     any    `json:"default"`
	Required    bool   `json:"required"`
	Deprecated  string `json:"deprecated,omitempty"`
}

type commandPrint struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Usage       string         `json:"usage"`
	Aliases     []string       `json:"aliases"`
	Examples    string         `json:"examples,omitempty"`
	Deprecated  string         `json:"deprecated,omitempty"`
	Flags       []flagPrint    `json:"flags"`
	Commands    []commandPrint `json:"commands"`
}

type rootPrint struct {
	Name        string         `json:"name"`
	Version     string         `json:"version"`
	Description string         `json:"description"`
	Usage       string         `json:"usage"`
	GlobalFlags []flagPrint    `json:"global_flags"`
	Commands    []commandPrint `json:"commands"`
}

func printTreeJSON(root *cobra.Command) ([]byte, error) {
	rp := rootPrint{
		Name:        root.Name(),
		Version:     version.Pretty(),
		Description: root.Short,
		Usage:       root.UseLine(),
		GlobalFlags: printFlagSet(root.PersistentFlags()),
		Commands:    printChildren(root),
	}
	return json.Marshal(rp)
}

func printTreeAndExit(root *cobra.Command) {
	b, err := printTreeJSON(root)
	out.MaybeDie(err, "unable to marshal command tree: %v", err)
	fmt.Fprintln(os.Stdout, string(b))
	os.Exit(0)
}

func printChildren(parent *cobra.Command) []commandPrint {
	children := parent.Commands()
	cp := make([]commandPrint, 0, len(children))
	for _, c := range children {
		if c.Hidden {
			continue
		}
		cp = append(cp, printCommand(c))
	}
	slices.SortFunc(cp, func(a, b commandPrint) int { return cmp.Compare(a.Name, b.Name) })
	return cp
}

func printCommand(c *cobra.Command) commandPrint {
	desc := c.Long
	if desc == "" {
		desc = c.Short
	}
	aliases := c.Aliases
	if aliases == nil {
		aliases = []string{}
	}
	return commandPrint{
		Name:        c.Name(),
		Description: desc,
		Usage:       c.UseLine(),
		Aliases:     aliases,
		Examples:    c.Example,
		Deprecated:  c.Deprecated,
		Flags:       printFlagSet(c.LocalFlags()),
		Commands:    printChildren(c),
	}
}

func printFlagSet(fs *pflag.FlagSet) []flagPrint {
	fp := []flagPrint{} // Assign to prevent printing `null` in the json output.
	fs.VisitAll(func(f *pflag.Flag) {
		if f.Hidden {
			return
		}
		if f.Name == "help" && f.Shorthand == "h" {
			return
		}
		fp = append(fp, printFlag(f))
	})
	return fp
}

func printFlag(f *pflag.Flag) flagPrint {
	return flagPrint{
		Name:        f.Name,
		Shorthand:   f.Shorthand,
		Type:        f.Value.Type(),
		Description: f.Usage,
		Default:     parseDefault(f),
		Required:    slices.Contains(f.Annotations[cobra.BashCompOneRequiredFlag], "true"),
		Deprecated:  f.Deprecated,
	}
}

func parseDefault(f *pflag.Flag) any {
	t := f.Value.Type()
	if strings.HasSuffix(t, "Slice") || strings.HasSuffix(t, "Array") {
		if f.DefValue == "" || f.DefValue == "[]" {
			return []any{}
		}
		return f.DefValue
	}
	if f.DefValue == "" {
		if t == "string" {
			return ""
		}
		return nil
	}
	switch t {
	case "bool":
		if b, err := strconv.ParseBool(f.DefValue); err == nil {
			return b
		}
	case "int", "int8", "int16", "int32", "int64",
		"uint", "uint8", "uint16", "uint32", "uint64", "count":
		if n, err := strconv.ParseInt(f.DefValue, 10, 64); err == nil {
			return n
		}
	case "float32", "float64":
		if x, err := strconv.ParseFloat(f.DefValue, 64); err == nil {
			return x
		}
	}
	return f.DefValue
}
