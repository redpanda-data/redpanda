// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package plugin

import (
	"fmt"
	"sync"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

var (
	managedMu sync.Mutex
	managed   = make(map[string]regManaged)
)

// ManagedHook is a hook to be called with the given fake-plugin-installed exec
// command and return a potentially wrapped command.
type ManagedHook func(*cobra.Command, afero.Fs) *cobra.Command

type regManaged struct {
	replaceArgs []string
	hook        ManagedHook
}

// RegisterManaged registers a plugin name to replacement args and a hook
// command.
//
// Replacement arguments replace the first argument of the plugin: Replacement
// args ["cloud", "byoc"] can replace "byoc" to install the byoc plugin at "rpk
// cloud byoc". Only the first argument is ever replaced, and if there are no
// replacement args, the plugin name is kept as is.
//
// The hook allows intercepting fake-plugin-exec-commands before the exec is
// actually ran. This can be used to login or attach extra flags.
func RegisterManaged(name string, replaceArgs []string, hook ManagedHook) {
	managedMu.Lock()
	defer managedMu.Unlock()
	if _, exists := managed[name]; exists {
		panic(fmt.Sprintf("doubly managed plugin %s", name))
	}
	managed[name] = regManaged{
		replaceArgs: replaceArgs,
		hook:        hook,
	}
}

// LookupManaged looks up if a plugin is managed. If so, this potentially
// replaces arguments and returns the managed hook to use on the command.
// If not, this returns the plugin unmodified and no hook.
func LookupManaged(plugin Plugin) (Plugin, ManagedHook) {
	managedMu.Lock()
	defer managedMu.Unlock()
	reg, ok := managed[plugin.Name]
	if !ok {
		return plugin, nil
	}
	if len(reg.replaceArgs) > 0 {
		plugin.Arguments = append(reg.replaceArgs, plugin.Arguments[1:]...)
	}
	return plugin, reg.hook
}
