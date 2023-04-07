// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package graf_test

import (
	"encoding/json"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/generate/graf"
	"github.com/stretchr/testify/require"
)

func TestSingleStatPanelType(t *testing.T) {
	panel := &graf.SingleStatPanel{}
	require.Equal(t, "singlestat", panel.Type())
}

func TestSingleStatPanelMarshalType(t *testing.T) {
	graphJSON, err := json.Marshal(&graf.SingleStatPanel{})
	require.NoError(t, err)
	require.Contains(t, string(graphJSON), `"type":"singlestat"`)
}
