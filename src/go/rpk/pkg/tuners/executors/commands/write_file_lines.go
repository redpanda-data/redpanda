package commands

import (
	"bufio"
	"fmt"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type writeFileLinesCommand struct {
	Command
	fs    afero.Fs
	path  string
	lines []string
}

func NewWriteFileLinesCmd(fs afero.Fs, path string, lines []string) Command {
	return &writeFileLinesCommand{
		fs:    fs,
		path:  path,
		lines: lines,
	}
}

func (c *writeFileLinesCommand) Execute() error {
	log.Debugf("Writing '%v' to file '%s'", c.lines, c.path)
	return utils.WriteFileLines(c.fs, c.lines, c.path)
}

func (c *writeFileLinesCommand) RenderScript(w *bufio.Writer) error {
	fmt.Fprintf(w, "cat << EOF > %s\n", c.path)
	for _, line := range c.lines {
		fmt.Fprint(w, "  ")
		fmt.Fprintln(w, line)
	}
	fmt.Fprintln(w, "EOF")
	return w.Flush()
}
