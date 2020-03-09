package cmd

import "testing"

func TestMergeFlags(t *testing.T) {
	tests := []struct {
		name      string
		current   map[string]string
		overrides []string
		expected  map[string]string
	}{
		{
			name:      "it should override the existent values",
			current:   map[string]string{"a": "true", "b": "2", "c": "127.0.0.1"},
			overrides: []string{"--a false", "b 42"},
			expected:  map[string]string{"a": "false", "b": "42", "c": "127.0.0.1"},
		}, {
			name:    "it should override the existent values (2)",
			current: map[string]string{"lock-memory": "true", "cpumask": "0-1", "logger-log-level": "'exception=debug'"},
			overrides: []string{"--overprovisioned", "--unsafe-bypass-fsync 1",
				"--default-log-level=trace", "--logger-log-level='exception=debug'",
				"--fail-on-abandoned-failed-futures"},
			expected: map[string]string{
				"lock-memory":                        "true",
				"cpumask":                            "0-1",
				"logger-log-level":                   "'exception=debug'",
				"overprovisioned":                    "",
				"unsafe-bypass-fsync":                "1",
				"default-log-level":                  "trace",
				"--fail-on-abandoned-failed-futures": "",
			},
		}, {
			name:      "it should create values not present in the current flags",
			current:   map[string]string{},
			overrides: []string{"b 42", "c 127.0.0.1"},
			expected:  map[string]string{"b": "42", "c": "127.0.0.1"},
		}, {
			name:      "it shouldn't change the current flags if no overrides are given",
			current:   map[string]string{"b": "42", "c": "127.0.0.1"},
			overrides: []string{},
			expected:  map[string]string{"b": "42", "c": "127.0.0.1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flags := mergeFlags(tt.current, tt.overrides)
			if len(flags) != len(tt.expected) {
				t.Fatal("the flags dicts differ in size")
			}

			for k, v := range flags {
				if tt.expected[k] != v {
					t.Fatalf(
						"expected value '%s' for key '%s', but got '%s'",
						tt.expected[k],
						k,
						v,
					)
				}
			}
		})
	}

}
