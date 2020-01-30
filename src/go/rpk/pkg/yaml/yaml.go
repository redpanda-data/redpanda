package yaml

import (
	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
)

func Persist(fs afero.Fs, in interface{}, file string) error {
	bytes, err := yaml.Marshal(in)
	if err != nil {
		return err
	}
	err = afero.WriteFile(fs, file, bytes, 0644)
	return err
}

func Read(fs afero.Fs, out interface{}, file string) error {
	content, err := afero.ReadFile(fs, file)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(content, out)
	if err != nil {
		return err
	}
	return nil
}
