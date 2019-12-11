package memory_test

import (
	"fmt"
	"testing"
	"vectorized/pkg/tuners/memory"
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
					[]byte(fmt.Sprint(memory.Swappiness)),
					memory.File,
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
					memory.File,
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
			checker := memory.NewSwappinessChecker(tt.fs)
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
