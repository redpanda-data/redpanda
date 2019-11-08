package commands_test

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"testing"
	"vectorized/pkg/tuners/executors/commands"

	"github.com/spf13/afero"
)

func TestWriteFileCmdExecute(t *testing.T) {
	fs := afero.NewMemMapFs()
	path := "/usr/file"
	content := `some
multiline 
content
`
	cmd := commands.NewWriteFileCmd(fs, path, content)
	if err := cmd.Execute(); err != nil {
		t.Errorf("an error happened while executing: %v", err)
	}
	file, err := fs.Open(path)
	if err != nil {
		t.Errorf("an error happened while opening the file: %v", err)
	}
	buf := make([]byte, len(content))
	if _, err = file.Read(buf); err != nil {
		t.Errorf("an error happened while reading the file: %v", err)
	}
	if !bytes.Equal(buf, []byte(content)) {
		t.Errorf("got:\n%s\nexpected:\n%s", string(buf), content)
	}
	info, err := file.Stat()
	if err != nil {
		t.Errorf("an error happened while running stat: %v", err)
	}
	expectedMode := os.FileMode(0644)
	if expectedMode != info.Mode() {
		t.Errorf("expected the file to have mode %v, got %v", expectedMode, info.Mode())
	}
}

func TestWriteFileModeCmdExecuteExistingFile(t *testing.T) {
	fs := afero.NewMemMapFs()
	path := "/usr/file"
	mode := os.FileMode(0765)

	err := afero.WriteFile(fs, path, []byte{}, mode)
	if err != nil {
		t.Errorf("got an error writing the file: %v", err)
	}

	// Execute it with a different mode to check that it preserves
	// the original mode
	cmd := commands.NewWriteFileModeCmd(fs, path, "", 0644)
	if err := cmd.Execute(); err != nil {
		t.Errorf("an error happened while executing: %v", err)
	}
	info, err := fs.Stat(path)
	if err != nil {
		t.Errorf("got an error trying to stat the file: %v", err)
	}
	if info.Mode() != mode {
		t.Errorf(
			"Execute changed the mode. Expected %o, got %o",
			uint32(mode),
			uint32(info.Mode()),
		)
	}
}

func TestWriteFileCmdRender(t *testing.T) {
	fs := afero.NewMemMapFs()
	path := "/usr/file"
	content := `some
multiline 
content
`
	cmd := commands.NewWriteFileCmd(fs, path, content)

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := cmd.RenderScript(writer); err != nil {
		t.Errorf("an error happened while rendering the script: %v", err)
	}

	expected := fmt.Sprintf(`echo '%s' > %s
chmod %o %s
`,
		content,
		path,
		0644,
		path,
	)

	if buf.String() != expected {
		t.Errorf("expected:\n\"%s\"\ngot:\n\"%s\"\n", expected, buf.String())
	}
}

func TestWriteFileCmdRenderExistingFile(t *testing.T) {
	fs := afero.NewMemMapFs()
	path := "/usr/file"
	content := "content"
	mode := os.FileMode(0765)

	// Create the file previously to check that the rendered
	// script doesn't include a chmod command, preserving
	// the existing file's mode.
	err := afero.WriteFile(fs, path, []byte(content), mode)
	if err != nil {
		t.Errorf("got an error writing the file: %v", err)
	}

	cmd := commands.NewWriteFileModeCmd(fs, path, content, 0777)

	var buf bytes.Buffer
	writer := bufio.NewWriter(&buf)
	if err := cmd.RenderScript(writer); err != nil {
		t.Errorf("got an error while rendering the script: %v", err)
	}

	expected := fmt.Sprintf(
		`echo '%s' > %s
`,
		content,
		path,
	)

	if buf.String() != expected {
		t.Errorf("expected:\n\"%s\"\ngot:\n\"%s\"\n", expected, buf.String())
	}
}
