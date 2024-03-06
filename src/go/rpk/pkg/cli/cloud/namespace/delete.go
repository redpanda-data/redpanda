package namespace

import (
	"fmt"

	"connectrpc.com/connect"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/oauth/providers/auth0"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	controlplanev1beta1 "github.com/redpanda-data/redpanda/src/go/rpk/proto/gen/go/redpanda/api/controlplane/v1beta1"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type deleteResponse struct {
	Name string `json:"name" yaml:"name"`
	ID   string `json:"id" yaml:"id"`
}

func deleteCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var noConfirm bool
	cmd := &cobra.Command{
		Use:   "delete [NAME]",
		Args:  cobra.ExactArgs(1),
		Short: "Delete Namespaces in Redpanda Cloud",
		Run: func(cmd *cobra.Command, args []string) {
			f := p.Formatter
			if h, ok := f.Help(deleteResponse{}); ok {
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

			name := args[0]
			listed, err := cl.Namespace.ListNamespaces(cmd.Context(), connect.NewRequest(&controlplanev1beta1.ListNamespacesRequest{
				Filter: &controlplanev1beta1.ListNamespacesRequest_Filter{Name: name},
			}))
			out.MaybeDie(err, "unable to find namespace %q: %v", name, err)
			if len(listed.Msg.Namespaces) == 0 {
				out.Die("unable to find namespace %q", name)
			}
			if len(listed.Msg.Namespaces) > 1 {
				// This is currently not possible, the filter is an exact
				// filter. This is just being cautious.
				out.Die("multiple namespaces were found for %q, please provide an exact match", name)
			}
			namespace := listed.Msg.Namespaces[0]
			if !noConfirm {
				confirmed, err := out.Confirm("Confirm deletion of namespace %q with ID %q?", namespace.Name, namespace.Id)
				out.MaybeDie(err, "unable to confirm deletion: %v", err)
				if !confirmed {
					out.Exit("Deletion canceled.")
				}
			}

			_, err = cl.Namespace.DeleteNamespace(cmd.Context(), connect.NewRequest(&controlplanev1beta1.DeleteNamespaceRequest{Id: namespace.Id}))
			out.MaybeDie(err, "unable to delete namespace %q: %v", name, err)
			res := deleteResponse{namespace.Name, namespace.Id}
			if isText, _, s, err := f.Format(res); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				fmt.Println(s)
				return
			}
			tw := out.NewTable("name", "id")
			defer tw.Flush()
			tw.PrintStructFields(res)
		},
	}
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "Disable confirmation prompt")
	return cmd
}
