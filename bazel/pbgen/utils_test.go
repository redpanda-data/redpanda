package main

import (
	"maps"
	"slices"
	"testing"
)

func TestToLower(t *testing.T) {
	testCases := map[string]string{
		"PascalCase":      "pascal_case",
		"RPCServer":       "rpc_server",
		"HTTPResponse":    "http_response",
		"ID":              "id",
		"MyID":            "my_id",
		"SimpleString":    "simple_string",
		"AnotherTest":     "another_test",
		"XMLHTTPRequest":  "xmlhttp_request", // How am I supposed to know?
		"APIClient":       "api_client",
		"URLParser":       "url_parser",
		"":                "",
		"A":               "a",
		"a":               "a",
		"CamelCaseString": "camel_case_string",
		"LongAcronymTEST": "long_acronym_test",
		"AcronymTESTWord": "acronym_test_word",
		"Edition2023Foo":  "edition2023_foo",
		"Foo123Bar":       "foo123_bar",
		"V1Beta1API":      "v1_beta1_api",
	}

	for input, expected := range testCases {
		result := pascalToSnakeCase(input)
		if result != expected {
			t.Errorf("converting %q to snake case, got: %q want: %q", input, result, expected)
		}
	}
}

func TestComponentSort(t *testing.T) {
	testCases := []struct {
		sorted []string
		graph  map[string][]string
	}{
		{
			graph: map[string][]string{
				"A": {"B"},
				"B": {"C"},
				"C": {"A"},
				"D": {"A"},
			},
			sorted: []string{"A", "B", "C", "D"},
		},
		{
			graph: map[string][]string{
				"A": {"B"},
				"B": {"C", "E", "F"},
				"C": {"D", "G"},
				"D": {"C", "H"},
				"E": {"A", "F"},
				"F": {"G"},
				"G": {"F"},
				"H": {"D", "G"},
			},
			sorted: []string{"G", "F", "C", "D", "H", "A", "B", "E"},
		},
		{
			graph: map[string][]string{
				"A": {"B"},
				"B": {"A"},
				"C": {"D"},
				"D": {"E"},
				"E": {"C"},
			},
			sorted: []string{"A", "B", "C", "D", "E"},
		},
		{
			graph: map[string][]string{
				"A": {"B"},
				"B": {"C"},
				"C": {"D"},
				"D": {"E"},
				"E": {},
			},
			sorted: []string{"E", "D", "C", "B", "A"},
		},
		{
			graph:  map[string][]string{},
			sorted: []string{},
		},
		{
			graph:  map[string][]string{"A": {}},
			sorted: []string{"A"},
		},
	}

	for _, tc := range testCases {
		nodes := slices.Collect(maps.Keys(tc.graph))
		slices.Sort(nodes) // Key iteration order is not guaranteed, so we sort the keys first.
		sorted := sortCyclicalGraph(nodes, func(id string) []string { return tc.graph[id] })
		if !slices.Equal(sorted, tc.sorted) {
			t.Errorf("expected sorted order %v, got %v", tc.sorted, sorted)
		}
	}
}
