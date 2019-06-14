package os

type Commands interface {
	Which(cmd string) (string, error)
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

func (commands *commands) Which(cmd string) (string, error) {
	out, err := commands.proc.RunWithSystemLdPath("which", cmd)
	if err == nil {
		return out[0], nil
	}
	return "", err
}
