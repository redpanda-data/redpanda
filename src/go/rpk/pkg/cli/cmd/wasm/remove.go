package wasm

import (
	"fmt"

	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/kafka"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/out"
)

func NewRemoveCommand(fs afero.Fs) *cobra.Command {
	var coprocType string

	cmd := &cobra.Command{
		Use:   "remove [NAME]",
		Short: "Remove inline WASM function.",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			err = checkCoprocType(coprocType)
			out.MaybeDieErr(err)
			err = ensureCoprocTopic(cl)
			out.MaybeDie(err, "coproc topic failure: %v", err)

			err = produceRemoveRecord(cl, args[0], coprocType)
			out.MaybeDie(err, "unable to produce remove message: %v", err)
			fmt.Println("Removal successful!")
		},
	}

	cmd.Flags().StringVar(&coprocType, "type", "async", "WASM engine type (async, data-policy)")

	return cmd
}
