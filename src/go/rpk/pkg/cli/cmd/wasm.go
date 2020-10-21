package cmd

import (
	wasm "vectorized/pkg/cli/cmd/wasm"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func NewWasmCommand(fs afero.Fs) *cobra.Command {
	command := &cobra.Command{
		Use:   "wasm",
		Short: "Create and upload inline WASM engine scripts",
	}
	command.AddCommand(wasm.NewGenerateCommand(fs))
	return command
}
