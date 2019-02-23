package utils

import (
	"bufio"
	"fmt"

	"github.com/spf13/afero"
)

func ReadFileLines(fs afero.Fs, filePath string) ([]string, error) {
	file, err := fs.Open(filePath)
	var lines []string
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return lines, nil
}

func ListFilesInPath(fs afero.Fs, path string) []string {
	var names []string
	file, _ := fs.Open(path)
	files, _ := file.Readdir(0)
	for _, fileInfo := range files {
		names = append(names, fileInfo.Name())
	}
	return names
}

func FileExists(fs afero.Fs, fileName string) bool {
	if _, err := fs.Stat(fileName); err == nil {
		return true
	}
	return false
}

func CopyFile(fs afero.Fs, src string, dst string) error {
	input, err := afero.ReadFile(fs, src)
	if err != nil {
		return err
	}
	err = afero.WriteFile(fs, dst, input, 0644)
	return err
}

func WriteFileLines(fs afero.Fs, lines []string, path string) error {
	file, err := fs.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		_, err := fmt.Fprintln(w, line)
		if err != nil {
			return err
		}
	}
	return w.Flush()
}
