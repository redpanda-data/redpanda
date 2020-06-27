package commands_test

import (
	"bufio"
	"bytes"
	"errors"
	"testing"
	"vectorized/pkg/system/systemd"
	"vectorized/pkg/tuners/executors/commands"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

const body = `[Unit]
Description=Foo

[Service]
ExecStart=/usr/sbin/foo-daemon

[Install]
WantedBy=multi-user.target
`

func TestInstallSystemdUnitCmdRender(t *testing.T) {
	cmd, err := commands.NewInstallSystemdUnitCmd(
		systemd.NewMockClient(nil, nil, nil, nil),
		afero.NewMemMapFs(),
		body,
		"foo.service",
	)
	require.NoError(t, err)

	expected := `cat << EOF > /etc/systemd/system/foo.service
` + body + `
EOF
sudo systemctl daemon-reload
`
	var buf bytes.Buffer

	w := bufio.NewWriter(&buf)
	cmd.RenderScript(w)
	require.NoError(t, w.Flush())

	require.Equal(t, expected, buf.String())
}

func TestInstallSystemdUnitCmdFail(t *testing.T) {
	returnedError := errors.New("some error")
	loadUnit := func(_ afero.Fs, _, _ string) error {
		return returnedError
	}
	client := systemd.NewMockClient(nil, nil, nil, loadUnit)

	cmd, err := commands.NewInstallSystemdUnitCmd(
		client,
		afero.NewMemMapFs(),
		body,
		"foo.service",
	)
	require.NoError(t, err)

	err = cmd.Execute()
	require.EqualError(t, err, returnedError.Error())
}
