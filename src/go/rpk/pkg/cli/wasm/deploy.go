package wasm

import (
	"fmt"
	"path/filepath"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newDeployCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		description string
		name        string
		coprocType  string
	)
	cmd := &cobra.Command{
		Use:   "deploy [PATH]",
		Short: "Deploy inline WASM function",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			path := args[0]
			if filepath.Ext(path) != ".js" {
				out.Die("cannot deploy %q: only .js files are supported", path)
			}

			err = checkCoprocType(coprocType)
			out.MaybeDieErr(err)
			err = ensureCoprocTopic(cl)
			out.MaybeDie(err, "coproc topic failure: %v", err)

			file, err := afero.ReadFile(fs, path)
			out.MaybeDie(err, "unable to read %q: %v", path, err)

			err = produceDeployRecord(cl, name, description, coprocType, file)
			out.MaybeDie(err, "unable to produce deploy message: %v", err)
			fmt.Println("Deploy successful!")
		},
	}

	cmd.Flags().StringVar(&description, "description", "", "Optional description about what the wasm function does")
	cmd.Flags().StringVar(&coprocType, "type", "async", "WASM engine type (async, data-policy)")
	cmd.Flags().StringVar(&name, "name", "", "Unique deploy identifier attached to the instance of this script")
	cmd.MarkFlagRequired("name")
	return cmd
}
