package config

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEditorAndOptions(t *testing.T) {
	testCases := map[string]struct {
		editor   string
		fileName string
		want     []string
	}{
		"default": {
			editor:   "code -w",
			fileName: "/tmp/one.yaml",
			want:     []string{"code", "-w", "/tmp/one.yaml"},
		},
		"noOptions": {
			editor:   "code",
			fileName: "/tmp/one.yaml",
			want:     []string{"code", "/tmp/one.yaml"},
		},
		"withQuotedOption": {
			editor:   "foo 'why would I do this'",
			fileName: "/tmp/one.yaml",
			want:     []string{"foo", "why would I do this", "/tmp/one.yaml"},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			got, err := editorAndArguments(tc.editor, tc.fileName)
			assert.NoError(t, err)
			if !reflect.DeepEqual(tc.want, got) {
				t.Fatalf("want %#v but got %#v", tc.want, got)
			}
		})
	}
}
