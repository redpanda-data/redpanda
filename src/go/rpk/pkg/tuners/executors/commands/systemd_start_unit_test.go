package commands_test

import (
	"bufio"
	"bytes"
	"errors"
	"testing"
	"vectorized/pkg/system/systemd"
	"vectorized/pkg/tuners/executors/commands"

	"github.com/stretchr/testify/require"
)

func TestStartSystemdUnitCmdRender(t *testing.T) {
	cmd, err := commands.NewStartSystemdUnitCmd(
		systemd.NewMockClient(nil, nil, nil, nil),
		"foo.service",
	)
	require.NoError(t, err)

	expected := `sudo systemctl start foo.service
`
	var buf bytes.Buffer

	w := bufio.NewWriter(&buf)
	cmd.RenderScript(w)
	require.NoError(t, w.Flush())

	require.Equal(t, expected, buf.String())
}

func TestStartSystemdUnitCmdFail(t *testing.T) {
	returnedError := errors.New("some error")
	startUnit := func(_ string) error {
		return returnedError
	}
	client := systemd.NewMockClient(nil, startUnit, nil, nil)

	cmd, err := commands.NewStartSystemdUnitCmd(
		client,
		"foo.service",
	)
	require.NoError(t, err)

	err = cmd.Execute()
	require.EqualError(t, err, returnedError.Error())
}
