package os

import "time"

type Commands interface {
	Which(cmd string, timeout time.Duration) (string, error)
}

func NewCommands(proc Proc) Commands {
	return &commands{
		proc: proc,
	}
}

type commands struct {
	Commands
	proc Proc
}

func (commands *commands) Which(
	cmd string, timeout time.Duration,
) (string, error) {
	out, err := commands.proc.RunWithSystemLdPath(timeout, "which", cmd)
	if err == nil {
		return out[0], nil
	}
	return "", err
}
