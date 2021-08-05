// Package plugin contains functions to load plugin information from a
// filesystem.
package plugin

import (
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
)

const (
	// NamePrefix is the expected prefix of the basename of plugins that do
	// not support the --help-autocomplete flag.
	NamePrefix = "rpk-"

	// NamePrefixAutoComplete is the expected prefix of the basename of
	// plugins that support the --help-autocomplete flag.
	NamePrefixAutoComplete = "rpk.ac-"

	// FlagAutoComplete is the expected flag if a plugin has the rpk.ac-
	// name prefix.
	FlagAutoComplete = "--help-autocomplete"
)

// Plugin groups information about a plugin's location on disk with the
// arguments to call the plugin.
//
// Plugins are searched in path order, and any unique name that comes first
// shadows any duplicate names.
//
// If a plugin filepath is /foo/bar/rpk.ac-baz-boz_biz, then the arguments will
// be baz-boz biz.
type Plugin struct {
	// Path is the path to the plugin binary on disk.
	Path string

	// Arguments is the set of arguments that should be used to call this
	// plugin.
	Arguments []string

	// ShadowedPaths are other plugin filepaths that are shadowed by this
	// plugin.
	ShadowedPaths []string
}

// NameToArgs converts a plugin command name into its arguments.
func NameToArgs(command string) []string {
	return strings.Split(command, "_")
}

// ListPlugins returns all plugins found in fs across the given search
// directories.
//
// Unlike kubectl, we do not allow plugins to be tacked on paths within other
// plugins. That is, we do not allow rpk_foo_bar to be an additional plugin on
// top of rpk_foo.
//
// We do support plugins defining themselves as "rpk-foo_bar", even though that
// reserves the "foo" plugin namespace.
func ListPlugins(fs afero.Fs, searchDirs []string) []Plugin {
	searchDirs = uniqueTrimmedStrs(searchDirs)

	uniquePlugins := make(map[string]int) // plugin name (e.g., mm3 or cloud) => index into plugins
	var plugins []Plugin
	for _, searchDir := range searchDirs {
		infos, err := afero.ReadDir(fs, searchDir)
		if err != nil {
			continue // unable to read dir; skip
		}
		for _, info := range infos {
			if info.IsDir() {
				continue
			}

			name := info.Name()
			if !strings.HasPrefix(name, NamePrefix) &&
				!strings.HasPrefix(name, NamePrefixAutoComplete) {
				continue // missing required name prefix; skip
			}

			fullPath := filepath.Join(searchDir, name)

			if info.Mode()&0111 == 0 {
				continue // not executable; skip
			}

			if strings.HasPrefix(name, NamePrefix) {
				name = strings.TrimPrefix(name, NamePrefix)
			} else {
				name = strings.TrimPrefix(name, NamePrefixAutoComplete)
			}
			if len(name) == 0 { // e.g., "rpk-"
				continue
			}

			arguments := NameToArgs(name)
			pluginName := arguments[0]

			priorAt, exists := uniquePlugins[pluginName]
			if exists {
				prior := &plugins[priorAt]
				prior.ShadowedPaths = append(prior.ShadowedPaths, fullPath)
				continue
			}

			uniquePlugins[pluginName] = len(plugins)
			plugins = append(plugins, Plugin{
				Path:      fullPath,
				Arguments: arguments,
			})

		}
	}

	return plugins
}

// Returns the unique strings in `in`, preserving order.
//
// Order preservation is important for search paths: a higher priority plugin
// (path search wise) will shadow a lower priority one.
func uniqueTrimmedStrs(in []string) []string {
	seen := make(map[string]bool)
	keep := in[:0]
	for i := 0; i < len(in); i++ {
		path := in[i]
		path = strings.TrimSpace(path)
		if seen[path] || len(path) == 0 {
			continue
		}
		seen[path] = true
		keep = append(keep, path)
	}
	return keep
}
