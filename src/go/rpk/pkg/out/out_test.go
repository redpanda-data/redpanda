package out

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrintStructFields(t *testing.T) {
	for _, test := range []struct {
		in  interface{}
		exp string
	}{
		{
			in: &struct {
				F1 string
			}{
				"foo",
			},

			exp: "foo\n",
		},

		{
			in: struct {
				F1 string
				F2 int
			}{
				"foo",
				3,
			},

			exp: "foo   3\n",
		},

		//
	} {
		b := new(bytes.Buffer)
		tw := NewTabWriterTo(b)
		tw.PrintStructFields(test.in)
		tw.Flush()
		got := b.String()
		require.Equal(t, test.exp, got, "not equal!")
	}
}
