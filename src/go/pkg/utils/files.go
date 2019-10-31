package utils

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"

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

func WriteBytes(fs afero.Fs, bs []byte, path string) (int, error) {
	file, err := fs.Create(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()
	return file.Write(bs)
}

func FileMd5(fs afero.Fs, filePath string) (string, error) {
	var returnMD5String string
	file, err := fs.Open(filePath)
	if err != nil {
		return returnMD5String, err
	}
	defer file.Close()
	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return returnMD5String, err
	}
	hashInBytes := hash.Sum(nil)
	returnMD5String = hex.EncodeToString(hashInBytes)
	return returnMD5String, nil
}

func BackupFile(fs afero.Fs, filePath string) (string, error) {
	md5, err := FileMd5(fs, filePath)
	if err != nil {
		return "", err
	}
	bkFilePath := fmt.Sprintf("%s.vectorized.%s.bk", filePath, md5)
	err = CopyFile(fs, filePath, bkFilePath)
	if err != nil {
		return "", fmt.Errorf("unable to create backup of %s", filePath)
	}
	return bkFilePath, nil
}
