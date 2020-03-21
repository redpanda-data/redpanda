package cmd

import (
	"bytes"
	"log"
	"strings"
	"testing"
	"vectorized/pkg/config"
	"vectorized/pkg/utils"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

func TestInteractive(t *testing.T) {
	tests := []struct {
		name           string
		input          string
		expectedOutput string
		expectErr      bool
	}{
		{
			name:  "it should return if the user enters 'n'",
			input: "n\n",
		},
		{
			name:           "it should run the tuner if the user enters 'y'",
			input:          "y\n",
			expectedOutput: "Running 'swappiness' tuner...",
		},
		{
			name:           "it should exit if the user enters 'q'",
			input:          "q\n",
			expectedOutput: "user exited",
			expectErr:      true,
		},
		{
			name:           "it should prompt again if the user enters something else",
			input:          "what\ny\n",
			expectedOutput: "Unrecognized option 'what'",
		},
		{
			name:           "it should prompt again if the user just hits 'return'",
			input:          "\ny\n",
			expectedOutput: "Please choose an option",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conf := config.DefaultConfig()
			fs := afero.NewMemMapFs()
			// Write invalid yaml to simulate an error
			_, err := utils.WriteBytes(fs, []byte("*asdf&"), conf.ConfigFile)
			if err != nil {
				log.Fatal(err)
			}
			_, err = utils.WriteBytes(fs, []byte("60"), "/proc/sys/vm/swappiness")
			if err != nil {
				t.Fatal(err)
			}
			var out bytes.Buffer
			in := strings.NewReader(tt.input)
			cmd := NewTuneCommand(fs)
			cmd.SetArgs([]string{"swappiness", "--interactive"})
			cmd.SetIn(in)
			logrus.SetOutput(&out)
			err = cmd.Execute()
			if tt.expectErr {
				if err == nil {
					t.Fatal("Expected an error, but got nil")
				}
				return
			} else {
				if err != nil {
					t.Fatalf("Got an unexpected error: %v", err)
				}
			}
			output := out.String()
			if !strings.Contains(output, tt.expectedOutput) {
				t.Fatalf(
					"expected output:\n\"%s\"\nto contain\n\"%s\"",
					output,
					tt.expectedOutput,
				)
			}
		})
	}
}
