package commands

import (
	"bufio"
	"fmt"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type backupFileCommand struct {
	Command
	fs   afero.Fs
	path string
}

func NewBackupFileCmd(fs afero.Fs, path string) Command {
	return &backupFileCommand{
		fs:   fs,
		path: path,
	}
}

func (c *backupFileCommand) Execute() error {
	log.Debugf("Creating backup of '%s'", c.path)
	bckFile, err := utils.BackupFile(c.fs, c.path)
	if err == nil {
		log.Debugf("Backup created '%s'", bckFile)
	}
	return err
}

func (c *backupFileCommand) RenderScript(w *bufio.Writer) error {
	fmt.Fprintf(w, "md_5=$(md5sum %s | awk '{print $1}')\n", c.path)
	fmt.Fprintf(w, "cp %s %s.vectorized.${md_5}.bk\n", c.path, c.path)
	return w.Flush()
}
