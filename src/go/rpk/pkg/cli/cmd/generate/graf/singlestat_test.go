package graf_test

import (
	"encoding/json"
	"testing"
	"vectorized/pkg/cli/cmd/generate/graf"

	"github.com/stretchr/testify/require"
)

func TestSingleStatPanelType(t *testing.T) {
	require.Equal(t, "singlestat", graf.SingleStatPanel{}.Type())
}

func TestSingleStatPanelMarshalType(t *testing.T) {
	graphJSON, err := json.Marshal(graf.SingleStatPanel{})
	require.NoError(t, err)
	require.Contains(t, string(graphJSON), `"type":"singlestat"`)
}
