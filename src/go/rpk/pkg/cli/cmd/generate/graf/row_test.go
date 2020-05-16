package graf_test

import (
	"encoding/json"
	"testing"
	"vectorized/pkg/cli/cmd/generate/graf"

	"github.com/stretchr/testify/require"
)

func TestRowPanelType(t *testing.T) {
	panel := &graf.RowPanel{}
	require.Equal(t, "row", panel.Type())
}

func TestRowPanelMarshalType(t *testing.T) {
	graphJSON, err := json.Marshal(&graf.RowPanel{})
	require.NoError(t, err)
	require.Contains(t, string(graphJSON), `"type":"row"`)
}
