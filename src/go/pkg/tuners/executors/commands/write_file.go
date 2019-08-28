package commands

import (
	"bufio"
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type writeFileCommand struct {
	Command
	fs      afero.Fs
	path    string
	content string
}

func NewWriteFileCmd(fs afero.Fs, path string, content string) Command {
	return &writeFileCommand{
		fs:      fs,
		path:    path,
		content: content,
	}
}

func (c *writeFileCommand) Execute() error {
	log.Debugf("Writing '%s' to file '%s'", c.content, c.path)
	return afero.WriteFile(c.fs, c.path, []byte(c.content), 0644)
}

func (c *writeFileCommand) RenderScript(w *bufio.Writer) error {
	fmt.Fprintf(w, "echo '%s' > %s\n", c.content, c.path)
	return w.Flush()
}
