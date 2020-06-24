package commands

import (
	"bufio"
	"fmt"
	"vectorized/pkg/system/systemd"
)

type startSystemdUnitCommand struct {
	client systemd.Client
	name   string
}

/*
 * Start a systemd unit with the provided name.
 */
func NewStartSystemdUnitCmd(
	client systemd.Client, name string,
) (Command, error) {
	return &startSystemdUnitCommand{client: client, name: name}, nil
}

func (cmd *startSystemdUnitCommand) Execute() error {
	return cmd.client.StartUnit(cmd.name)
}

func (cmd *startSystemdUnitCommand) RenderScript(w *bufio.Writer) error {
	_, err := fmt.Fprintf(w, "sudo systemctl start %s\n", cmd.name)
	return err
}
