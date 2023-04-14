package wasm

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newRemoveCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var coprocType string

	cmd := &cobra.Command{
		Use:   "remove [NAME]",
		Short: "Remove inline WASM function",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
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
