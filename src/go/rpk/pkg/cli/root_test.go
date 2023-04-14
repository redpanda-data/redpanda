package cli

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
)

func TestAddPluginWithExec(t *testing.T) {
	root := &cobra.Command{
		Use: "root",
	}

	subcommands := func(c *cobra.Command) []string {
		var subs []string
		for _, c := range c.Commands() {
			subs = append(subs, c.Use)
		}
		return subs
	}
	find := func(c *cobra.Command, search []string) *cobra.Command {
		child, _, _ := c.Find(search)
		return child
	}

	addPluginWithExec(root, "foo", []string{"foo", "bar"}, "", nil, nil) // we cannot test exec path, so we leave empty
	assert.Equal(t, []string{"foo"}, subcommands(root), "expected one subcommand of root to be [foo]")
	assert.Equal(t, []string{"bar"}, subcommands(find(root, []string{"foo"})), "expected one subcommand of root foo to be [bar]")

	addPluginWithExec(root, "foo", []string{"foo", "baz"}, "", nil, nil)
	assert.Equal(t, []string{"foo"}, subcommands(root), "expected one subcommand of root to still be [foo]")
	assert.Equal(t, []string{"bar", "baz"}, subcommands(find(root, []string{"foo"})), "expected two subcommands of root foo to be [bar, baz]")

	addPluginWithExec(root, "fizz", []string{"fizz"}, "", nil, nil)
	assert.Equal(t, []string{"fizz", "foo"}, subcommands(root), "expected one subcommand of root to be [fizz, foo]")
	assert.Equal(t, []string{"bar", "baz"}, subcommands(find(root, []string{"foo"})), "expected two subcommands of root foo to still be [bar, baz]")
	assert.Equal(t, []string(nil), subcommands(find(root, []string{"fizz"})), "expected no subcommands under fizz")
}

func TestTrackHelp(t *testing.T) {
	var base useHelp

	trackHelp(&base, []string{"foo", "bar"}, pluginHelp{Short: "short foo bar"})
	exp := useHelp{
		inner: map[string]*useHelp{
			"foo": {
				inner: map[string]*useHelp{
					"bar": {help: pluginHelp{Short: "short foo bar"}},
				},
			},
		},
	}
	assert.Equal(t, exp, base, "expected trackHelp to create two nestings for %s", []string{"foo", "bar"})

	trackHelp(&base, []string{"foo", "baz"}, pluginHelp{Short: "short foo baz"})
	exp.inner["foo"].inner["baz"] = &useHelp{help: pluginHelp{Short: "short foo baz"}}
	assert.Equal(t, exp, base, "expected trackHelp to create additional second-level nesting for %s", []string{"foo", "baz"})

	trackHelp(&base, []string{"biz"}, pluginHelp{Short: "top level biz"})
	exp.inner["biz"] = &useHelp{help: pluginHelp{Short: "top level biz"}}
	assert.Equal(t, exp, base, "expected trackHelp to create additional top-level command for %s", []string{"biz"})

	trackHelp(&base, []string{"foo"}, pluginHelp{Short: "updated foo help"})
	exp.inner["foo"].help = pluginHelp{Short: "updated foo help"}
	assert.Equal(t, exp, base, "expected trackHelp to create perform in-place update to existing incomplete top level-foo")
}
