package namespace

import (
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type listResponse struct {
	Name string `json:"name" yaml:"name"`
	ID   string `json:"id" yaml:"id"`
}

func listCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Args:  cobra.ExactArgs(0),
		Short: "List Namespaces in Redpanda Cloud",
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]listResponse{}); ok {
				out.Exit(h)
			}
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			priorProfile := cfg.ActualProfile()
			_, authVir, clearedProfile, _, err := oauth.LoadFlow(cmd.Context(), fs, cfg, auth0.NewClient(cfg.DevOverrides()), false, false, cfg.DevOverrides().CloudAPIURL)
			out.MaybeDie(err, "unable to authenticate with Redpanda Cloud: %v", err)

			oauth.MaybePrintSwapMessage(clearedProfile, priorProfile, authVir)
			authToken := authVir.AuthToken
			cl, err := publicapi.NewClientSet(cfg.DevOverrides().PublicAPIURL, authToken)
			out.MaybeDie(err, "unable to create the public api client: %v", err)

			namespaces, err := listAllNamespaces(cmd.Context(), cl)
			out.MaybeDie(err, "unable to list namespaces: %v", err)

			if isText, _, s, err := f.Format(namespaces); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				fmt.Println(s)
				return
			}
			tw := out.NewTable("name", "id")
			defer tw.Flush()
			for _, n := range namespaces {
				tw.PrintStructFields(n)
			}
		},
	}
}
