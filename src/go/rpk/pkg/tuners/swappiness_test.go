package tuners_test

import (
	"fmt"
	"strings"
	"testing"
	"vectorized/pkg/tuners"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/utils"

	"github.com/spf13/afero"
)

func TestChecker(t *testing.T) {
	tests := []struct {
		name      string
		fs        afero.Fs
		before    func(fs afero.Fs) error
		expectOk  bool
		expectErr bool
	}{
		{
			name: "It should return true if the value is correct",
			fs:   afero.NewMemMapFs(),
			before: func(fs afero.Fs) error {
				_, err := utils.WriteBytes(
					fs,
					[]byte(fmt.Sprint(tuners.ExpectedSwappiness)),
					tuners.File,
				)
				return err
			},
			expectOk: true,
		},
		{
			name: "It should return false if the file exists but the value iswrong",
			fs:   afero.NewMemMapFs(),
			before: func(fs afero.Fs) error {
				_, err := utils.WriteBytes(
					fs,
					[]byte("120"),
					tuners.File,
				)
				return err
			},
			expectOk: false,
		},
		{
			name:      "It should fail if the file doesn't exist",
			fs:        afero.NewMemMapFs(),
			expectOk:  false,
			expectErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.before != nil {
				err := tt.before(tt.fs)
				if err != nil {
					t.Errorf("got an error setting up the test: %v", err)
				}
			}
			checker := tuners.NewSwappinessChecker(tt.fs)
			res := checker.Check()
			if !tt.expectErr && res.Err != nil {
				t.Errorf("got an unexpected error: %v", res.Err)
			}
			if tt.expectOk != res.IsOk {
				t.Errorf(
					"expected checker to return %t, but got %t",
					tt.expectOk,
					res.IsOk,
				)
			}
		})
	}
}

func TestTuner(t *testing.T) {
	tests := []struct {
		name      string
		fs        afero.Fs
		before    func(fs afero.Fs) error
		expectErr bool
	}{
		{
			name: "It should leave the same value if it was correct",
			fs:   afero.NewMemMapFs(),
			before: func(fs afero.Fs) error {
				_, err := utils.WriteBytes(
					fs,
					[]byte(fmt.Sprint(tuners.ExpectedSwappiness)),
					tuners.File,
				)
				return err
			},
		},
		{
			name: "It should change the value if it was different",
			fs:   afero.NewMemMapFs(),
			before: func(fs afero.Fs) error {
				_, err := utils.WriteBytes(
					fs,
					[]byte("120"),
					tuners.File,
				)
				return err
			},
		},
		{
			name:      "It should fail if the file doesn't exist",
			fs:        afero.NewMemMapFs(),
			expectErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.before != nil {
				err := tt.before(tt.fs)
				if err != nil {
					t.Errorf(
						"got an error setting up the test: %v",
						err,
					)
				}
			}
			tuner := tuners.NewSwappinessTuner(tt.fs, executors.NewDirectExecutor())
			res := tuner.Tune()
			if res.Error() != nil {
				if !tt.expectErr {
					t.Errorf(
						"got an unexpected error: %v",
						res.Error(),
					)
				}
				return
			}
			lines, err := utils.ReadFileLines(tt.fs, tuners.File)
			if err != nil {
				t.Errorf(
					"got an error while reading back the file: %v",
					err,
				)
			}
			if len(lines) != 1 {
				t.Errorf("expected 1 line, got %d", len(lines))
				return
			}
			if lines[0] != fmt.Sprint(tuners.ExpectedSwappiness) {
				t.Errorf(
					"expected the file contents to be '1', but got '%s'",
					strings.Join(lines, "\n"),
				)
			}
		})
	}
}
