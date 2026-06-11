package out

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type renderFixture struct {
	Name  string `json:"name"  yaml:"name"  table:"NAME"`
	Count int    `json:"count" yaml:"count" table:"COUNT"`
}

// testFormatter is a local implementation of Formatter for unit tests,
// avoiding the import cycle that would arise from using config.OutFormatter.
type testFormatter struct {
	kind string
}

func (f testFormatter) Format(v any) (isText, isWide bool, s string, err error) {
	switch f.kind {
	case "json":
		b, err := json.Marshal(v)
		if err != nil {
			return false, false, "", err
		}
		return false, false, string(b), nil
	case "yaml":
		b, err := yaml.Marshal(v)
		if err != nil {
			return false, false, "", err
		}
		return false, false, string(b), nil
	case "text":
		return true, false, "", nil
	case "wide":
		return true, true, "", nil
	default:
		return false, false, "", fmt.Errorf("--format %q not supported", f.kind)
	}
}

func TestParseTableTag(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want tableTag
	}{
		{name: "empty", in: "", want: tableTag{}},
		{name: "dash", in: "-", want: tableTag{present: true, skip: true}},
		{name: "header only", in: "NAME", want: tableTag{present: true, header: "NAME"}},
		{name: "wide", in: "NAME,wide", want: tableTag{present: true, header: "NAME", wide: true}},
		{name: "omitempty", in: "NAME,omitempty", want: tableTag{present: true, header: "NAME", omitempty: true}},
		{name: "wide and omitempty", in: "NAME,wide,omitempty", want: tableTag{present: true, header: "NAME", wide: true, omitempty: true}},
		{name: "unknown modifier ignored", in: "NAME,bogus", want: tableTag{present: true, header: "NAME"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, parseTableTag(tc.in))
		})
	}
}

func TestParseHeaderTag(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want headerTag
	}{
		{name: "empty", in: "", want: headerTag{}},
		{name: "title only", in: "SUMMARY", want: headerTag{present: true, title: "SUMMARY"}},
		{name: "omitempty", in: "SUMMARY,omitempty", want: headerTag{present: true, title: "SUMMARY", omitempty: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, parseHeaderTag(tc.in))
		})
	}
}

func TestRenderPassesThroughJSON(t *testing.T) {
	f := testFormatter{kind: "json"}
	var buf bytes.Buffer
	err := Render(f, &buf, renderFixture{Name: "foo", Count: 3})
	require.NoError(t, err)
	require.JSONEq(t, `{"name":"foo","count":3}`, strings.TrimSpace(buf.String()))
}

func TestRenderPassesThroughYAML(t *testing.T) {
	f := testFormatter{kind: "yaml"}
	var buf bytes.Buffer
	err := Render(f, &buf, renderFixture{Name: "foo", Count: 3})
	require.NoError(t, err)
	require.Equal(t, "name: foo\ncount: 3\n\n", buf.String())
}

func TestRenderUnsupportedFormatReturnsError(t *testing.T) {
	f := testFormatter{kind: "xml"}
	var buf bytes.Buffer
	err := Render(f, &buf, renderFixture{})
	require.Error(t, err)
}

type listRow struct {
	Name       string `json:"name"       yaml:"name"       table:"NAME"`
	Partitions int    `json:"partitions" yaml:"partitions" table:"PARTITIONS"`
	Replicas   int    `json:"replicas"   yaml:"replicas"   table:"REPLICAS"`
}

func TestRenderListBasicReflection(t *testing.T) {
	f := testFormatter{kind: "text"}
	var buf bytes.Buffer
	err := Render(f, &buf, []listRow{
		{Name: "alpha", Partitions: 3, Replicas: 7},
		{Name: "beta", Partitions: 1, Replicas: 5},
	})
	require.NoError(t, err)

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	require.Len(t, lines, 3) // header + 2 rows
	// Header row preserves the declared table: labels (already uppercase here).
	require.Contains(t, lines[0], "NAME")
	require.Contains(t, lines[0], "PARTITIONS")
	require.Contains(t, lines[0], "REPLICAS")
	require.Contains(t, lines[1], "alpha")
	require.Contains(t, lines[1], "3")
	require.Contains(t, lines[1], "7")
	require.Contains(t, lines[2], "beta")
	require.Contains(t, lines[2], "1")
	require.Contains(t, lines[2], "5")
}

func TestRenderListEmptySlicePrintsHeadersOnly(t *testing.T) {
	f := testFormatter{kind: "text"}
	var buf bytes.Buffer
	err := Render(f, &buf, []listRow{})
	require.NoError(t, err)
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	require.Len(t, lines, 1)
	require.Contains(t, lines[0], "NAME")
}

func TestRenderListSkipsDashTag(t *testing.T) {
	type row struct {
		Name   string `json:"name"   yaml:"name"   table:"NAME"`
		Hidden string `json:"hidden" yaml:"hidden" table:"-"`
	}
	f := testFormatter{kind: "text"}
	var buf bytes.Buffer
	err := Render(f, &buf, []row{{Name: "a", Hidden: "secret"}})
	require.NoError(t, err)
	require.NotContains(t, buf.String(), "secret")
	require.NotContains(t, buf.String(), "HIDDEN")
}

type wideRow struct {
	Name   string `json:"name"   yaml:"name"   table:"NAME"`
	Bytes  int64  `json:"bytes"  yaml:"bytes"  table:"LOG BYTES,wide"`
	Detail string `json:"detail" yaml:"detail" table:"DETAIL,wide"`
}

func TestRenderListHidesWideByDefault(t *testing.T) {
	f := testFormatter{kind: "text"}
	var buf bytes.Buffer
	err := Render(f, &buf, []wideRow{{Name: "x", Bytes: 1024, Detail: "hi"}})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "NAME")
	require.NotContains(t, buf.String(), "LOG BYTES")
	require.NotContains(t, buf.String(), "DETAIL")
	require.NotContains(t, buf.String(), "1024")
}

func TestRenderListShowsWideWhenWide(t *testing.T) {
	f := testFormatter{kind: "wide"}
	var buf bytes.Buffer
	err := Render(f, &buf, []wideRow{{Name: "x", Bytes: 1024, Detail: "hi"}})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "NAME")
	require.Contains(t, buf.String(), "LOG BYTES")
	require.Contains(t, buf.String(), "DETAIL")
	require.Contains(t, buf.String(), "1024")
}

type objectFixture struct {
	Name       string `json:"name"       yaml:"name"       table:"NAME"`
	Internal   bool   `json:"internal"   yaml:"internal"   table:"INTERNAL"`
	Partitions int    `json:"partitions" yaml:"partitions" table:"PARTITIONS"`
}

func TestRenderObjectBasic(t *testing.T) {
	f := testFormatter{kind: "text"}
	var buf bytes.Buffer
	err := Render(f, &buf, objectFixture{Name: "my-topic", Internal: false, Partitions: 3})
	require.NoError(t, err)
	require.Equal(t, [][]string{
		{"NAME", "my-topic"},
		{"INTERNAL", "false"},
		{"PARTITIONS", "3"},
	}, TableRows(buf.String()))
}

func TestRenderObjectSkipsDashTag(t *testing.T) {
	type obj struct {
		Name   string `json:"name"   yaml:"name"   table:"NAME"`
		Hidden string `json:"hidden" yaml:"hidden" table:"-"`
	}
	f := testFormatter{kind: "text"}
	var buf bytes.Buffer
	err := Render(f, &buf, obj{Name: "a", Hidden: "secret"})
	require.NoError(t, err)
	require.NotContains(t, buf.String(), "secret")
	require.NotContains(t, buf.String(), "HIDDEN")
}

type objectOmit struct {
	Name     string `json:"name"     yaml:"name"     table:"NAME"`
	Internal bool   `json:"internal" yaml:"internal" table:"INTERNAL,omitempty"`
	Error    string `json:"error"    yaml:"error"    table:"ERROR,omitempty"`
}

func TestRenderObjectOmitEmptySkipsZero(t *testing.T) {
	f := testFormatter{kind: "text"}
	var buf bytes.Buffer
	err := Render(f, &buf, objectOmit{Name: "a"})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "NAME")
	require.NotContains(t, buf.String(), "INTERNAL")
	require.NotContains(t, buf.String(), "ERROR")
}

func TestRenderObjectOmitEmptyKeepsNonZero(t *testing.T) {
	f := testFormatter{kind: "text"}
	var buf bytes.Buffer
	err := Render(f, &buf, objectOmit{Name: "a", Internal: true, Error: "oops"})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "INTERNAL")
	require.Contains(t, buf.String(), "true")
	require.Contains(t, buf.String(), "ERROR")
	require.Contains(t, buf.String(), "oops")
}

type compositeSummary struct {
	Name       string `json:"name"       yaml:"name"       table:"NAME"`
	Partitions int    `json:"partitions" yaml:"partitions" table:"PARTITIONS"`
}

type compositeCfg struct {
	Key   string `json:"key"   yaml:"key"   table:"KEY"`
	Value string `json:"value" yaml:"value" table:"VALUE"`
}

type compositeFixture struct {
	Summary compositeSummary `json:"summary" yaml:"summary" header:"SUMMARY"`
	Configs []compositeCfg   `json:"configs" yaml:"configs" header:"CONFIGS"`
}

func TestRenderCompositeBasic(t *testing.T) {
	f := testFormatter{kind: "text"}
	var buf bytes.Buffer
	err := Render(f, &buf, compositeFixture{
		Summary: compositeSummary{Name: "my-topic", Partitions: 3},
		Configs: []compositeCfg{
			{Key: "cleanup.policy", Value: "delete"},
			{Key: "compression.type", Value: "producer"},
		},
	})
	require.NoError(t, err)

	out := buf.String()
	require.Contains(t, out, "SUMMARY")
	require.Contains(t, out, "=======") // underline matching len("SUMMARY") == 7
	require.Contains(t, out, "my-topic")
	require.Contains(t, out, "CONFIGS")
	require.Contains(t, out, "cleanup.policy")
	require.Contains(t, out, "delete")

	// SUMMARY section appears before CONFIGS section.
	require.Less(t, strings.Index(out, "SUMMARY"), strings.Index(out, "CONFIGS"))
}

func TestRenderCompositeSkipsFieldsWithoutHeader(t *testing.T) {
	type fixture struct {
		Summary compositeSummary `json:"summary" yaml:"summary" header:"SUMMARY"`
		Hidden  compositeSummary `json:"hidden"  yaml:"hidden"`
	}
	f := testFormatter{kind: "text"}
	var buf bytes.Buffer
	err := Render(f, &buf, fixture{
		Summary: compositeSummary{Name: "shown"},
		Hidden:  compositeSummary{Name: "not-shown"},
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "shown")
	require.NotContains(t, buf.String(), "not-shown")
}

type compositeOmitFixture struct {
	Summary compositeSummary `json:"summary" yaml:"summary" header:"SUMMARY,omitempty"`
	Configs []compositeCfg   `json:"configs" yaml:"configs" header:"CONFIGS,omitempty"`
}

func TestRenderCompositeOmitEmptySkipsZeroStruct(t *testing.T) {
	f := testFormatter{kind: "text"}
	var buf bytes.Buffer
	err := Render(f, &buf, compositeOmitFixture{
		// Summary is zero value.
		Configs: []compositeCfg{{Key: "k", Value: "v"}},
	})
	require.NoError(t, err)
	require.NotContains(t, buf.String(), "SUMMARY")
	require.Contains(t, buf.String(), "CONFIGS")
	require.Contains(t, buf.String(), "k")
}

func TestRenderCompositeOmitEmptySkipsEmptySlice(t *testing.T) {
	f := testFormatter{kind: "text"}
	var buf bytes.Buffer
	err := Render(f, &buf, compositeOmitFixture{
		Summary: compositeSummary{Name: "t"},
		// Configs is nil.
	})
	require.NoError(t, err)
	require.Contains(t, buf.String(), "SUMMARY")
	require.NotContains(t, buf.String(), "CONFIGS")
}

type exampleTopic struct {
	Name       string `json:"name"       yaml:"name"       table:"NAME"`
	Partitions int    `json:"partitions" yaml:"partitions" table:"PARTITIONS"`
}

func TestRenderListOfComposites(t *testing.T) {
	f := testFormatter{kind: "text"}
	var buf bytes.Buffer
	err := Render(f, &buf, []compositeFixture{
		{
			Summary: compositeSummary{Name: "a", Partitions: 1},
			Configs: []compositeCfg{{Key: "k", Value: "v"}},
		},
		{
			Summary: compositeSummary{Name: "b", Partitions: 2},
			Configs: []compositeCfg{{Key: "k2", Value: "v2"}},
		},
	})
	require.NoError(t, err)
	out := buf.String()
	require.Contains(t, out, "SUMMARY")
	require.Contains(t, out, "CONFIGS")
	require.Contains(t, out, "a")
	require.Contains(t, out, "b")
	// Two composite blocks separated by blank line — b appears after a.
	require.Less(t, strings.Index(out, "a"), strings.Index(out, "b"))
}

func ExampleRender_list() {
	f := testFormatter{kind: "text"}
	rows := []exampleTopic{
		{Name: "orders", Partitions: 3},
		{Name: "clicks", Partitions: 8},
	}
	_ = Render(f, os.Stdout, rows)
	// Output:
	// NAME    PARTITIONS
	// orders  3
	// clicks  8
}

func ExampleRender_object() {
	f := testFormatter{kind: "text"}
	one := exampleTopic{Name: "orders", Partitions: 3}
	_ = Render(f, os.Stdout, one)
	// Output:
	// NAME        orders
	// PARTITIONS  3
}

type exampleComposite struct {
	Summary exampleTopic   `json:"summary" yaml:"summary" header:"SUMMARY"`
	Peers   []exampleTopic `json:"peers"   yaml:"peers"   header:"PEERS"`
}

func ExampleRender_composite() {
	f := testFormatter{kind: "text"}
	c := exampleComposite{
		Summary: exampleTopic{Name: "orders", Partitions: 3},
		Peers:   []exampleTopic{{Name: "clicks", Partitions: 8}},
	}
	_ = Render(f, os.Stdout, c)
	// Output:
	// SUMMARY
	// =======
	// NAME        orders
	// PARTITIONS  3
	//
	// PEERS
	// =====
	// NAME    PARTITIONS
	// clicks  8
}
