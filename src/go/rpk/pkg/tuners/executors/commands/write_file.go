package commands

import (
	"bufio"
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

const defaultMode os.FileMode = 0644

type writeFileCommand struct {
	fs      afero.Fs
	path    string
	content string
	mode    os.FileMode
}

func NewWriteFileModeCmd(
	fs afero.Fs, path string, content string, mode os.FileMode,
) Command {
	return &writeFileCommand{fs, path, content, mode}
}

func NewWriteFileCmd(fs afero.Fs, path string, content string) Command {
	return NewWriteFileModeCmd(fs, path, content, defaultMode)
}

func (c *writeFileCommand) Execute() error {
	log.Debugf("Writing '%s' to file '%s'", c.content, c.path)
	mode := c.mode
	info, err := c.fs.Stat(c.path)
	if err != nil {
		// Ignore the error if the file doesn't exist
		if !os.IsNotExist(err) {
			return err
		}
	} else {
		mode = info.Mode()
	}
	file, err := c.fs.OpenFile(c.path, os.O_CREATE|os.O_TRUNC|os.O_RDWR|os.O_SYNC, mode)
	if err != nil {
		return err
	}
	defer file.Close()
	n, err := file.WriteString(c.content)
	if err != nil {
		return err
	}
	contentLength := len(c.content)
	if n != contentLength {
		return fmt.Errorf("wrote less bytes than expected: %d out of %d", n, contentLength)
	}
	return nil
}

func (c *writeFileCommand) RenderScript(w *bufio.Writer) error {
	fmt.Fprintf(w, "echo '%s' > %s\n", c.content, c.path)
	_, err := c.fs.Stat(c.path)
	// If the file doesn't exist, include a chmod command to set
	// its mode.
	if os.IsNotExist(err) {
		fmt.Fprintf(w, "chmod %o %s\n", uint32(c.mode), c.path)
	}
	return w.Flush()
}
