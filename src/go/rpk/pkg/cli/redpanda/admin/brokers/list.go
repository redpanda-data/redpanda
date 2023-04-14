package brokers

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List the brokers in your cluster",
		Args:    cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			bs, err := cl.Brokers(cmd.Context())
			out.MaybeDie(err, "unable to request brokers: %v", err)

			headers := []string{"Node-ID", "Num-Cores", "Membership-Status"}

			args := func(b *admin.Broker) []interface{} {
				ret := []interface{}{b.NodeID, b.NumCores, b.MembershipStatus}
				return ret
			}
			for _, b := range bs {
				if b.IsAlive != nil {
					headers = append(headers, "Is-Alive", "Broker-Version")
					orig := args
					args = func(b *admin.Broker) []interface{} {
						return append(orig(b), *b.IsAlive, b.Version)
					}
					break
				}
			}
			tw := out.NewTable(headers...)
			defer tw.Flush()
			for _, b := range bs {
				tw.Print(args(&b)...)
			}
		},
	}
}
