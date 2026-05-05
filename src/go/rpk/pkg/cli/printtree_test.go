package cli

import (
	"encoding/json"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func newPrintTreeTestRoot() *cobra.Command {
	root := &cobra.Command{
		Use:               "rpk",
		Short:             "rpk root",
		CompletionOptions: cobra.CompletionOptions{DisableDefaultCmd: true},
	}
	root.SetHelpCommand(&cobra.Command{Use: "no-help", Hidden: true})
	root.PersistentFlags().StringP("config", "c", "", "config file")
	root.PersistentFlags().BoolP("verbose", "v", false, "verbose")

	topic := &cobra.Command{
		Use:     "topic",
		Short:   "manage topics",
		Long:    "manage topics in detail",
		Aliases: []string{"t"},
		Example: "rpk topic create foo",
	}
	topic.Flags().Int("partitions", 3, "number of partitions")
	topic.Flags().StringP("name", "n", "", "topic name")
	topic.MarkFlagRequired("name")
	topic.Flags().StringSlice("tags", nil, "topic tags")
	topic.Flags().StringArray("entries", nil, "topic config entries")
	topic.Flags().String("internal", "", "internal use")
	topic.Flags().MarkHidden("internal")
	topic.Flags().String("legacy", "old", "legacy flag")
	topic.Flags().Lookup("legacy").Deprecated = "use --new-flag"

	short := &cobra.Command{Use: "short-only", Short: "short fallback"}
	old := &cobra.Command{Use: "old", Short: "old", Deprecated: "use new"}
	hidden := &cobra.Command{Use: "secret", Short: "hidden", Hidden: true}

	root.AddCommand(topic, short, old, hidden)
	return root
}

func TestPrintTree(t *testing.T) {
	root := newPrintTreeTestRoot()

	b, err := printTreeJSON(root)
	require.NoError(t, err)

	var got rootPrint
	require.NoError(t, json.Unmarshal(b, &got))

	require.Equal(t, "rpk", got.Name)
	require.Equal(t, "rpk root", got.Description)

	globals := flagsByName(got.GlobalFlags)
	require.Len(t, got.GlobalFlags, 2)
	require.Equal(t, "c", globals["config"].Shorthand)
	require.Equal(t, "string", globals["config"].Type)
	require.Equal(t, "bool", globals["verbose"].Type)
	require.Equal(t, false, globals["verbose"].Default)

	names := make([]string, len(got.Commands))
	for i, c := range got.Commands {
		names[i] = c.Name
	}
	require.Equal(t, []string{"old", "short-only", "topic"}, names, "children must be sorted and exclude hidden")

	cmds := commandsByName(got.Commands)
	require.Equal(t, "use new", cmds["old"].Deprecated)
	require.Equal(t, "short fallback", cmds["short-only"].Description, "Short is used when Long is empty")
	require.Equal(t, []string{}, cmds["short-only"].Aliases, "nil aliases must marshal as []")

	tc := cmds["topic"]
	require.Equal(t, "manage topics in detail", tc.Description, "Long is preferred over Short")
	require.Equal(t, []string{"t"}, tc.Aliases)
	require.Equal(t, "rpk topic create foo", tc.Examples)

	flags := flagsByName(tc.Flags)
	require.Len(t, tc.Flags, 5, "hidden flag must be excluded")
	require.Equal(t, "int", flags["partitions"].Type)
	require.Equal(t, float64(3), flags["partitions"].Default)
	require.False(t, flags["partitions"].Required)
	require.True(t, flags["name"].Required)
	require.Equal(t, "", flags["name"].Default)
	require.Equal(t, "n", flags["name"].Shorthand)
	require.Equal(t, []any{}, flags["tags"].Default)
	require.Equal(t, []any{}, flags["entries"].Default)
	require.Equal(t, "use --new-flag", flags["legacy"].Deprecated)

	raw := string(b)
	require.NotContains(t, raw, `"deprecated":""`, "empty deprecated must be omitted")
	require.NotContains(t, raw, `"examples":""`, "empty examples must be omitted")
}

func flagsByName(fs []flagPrint) map[string]flagPrint {
	m := make(map[string]flagPrint, len(fs))
	for _, f := range fs {
		m[f.Name] = f
	}
	return m
}

func commandsByName(cs []commandPrint) map[string]commandPrint {
	m := make(map[string]commandPrint, len(cs))
	for _, c := range cs {
		m[c.Name] = c
	}
	return m
}
