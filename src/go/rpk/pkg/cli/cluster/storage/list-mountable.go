package storage

import (
	"fmt"
	"io"
	"os"

	dataplanev1alpha2 "buf.build/gen/go/redpandadata/dataplane/protocolbuffers/go/redpanda/api/dataplane/v1alpha2"
	"connectrpc.com/connect"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newListMountable(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "list-mountable",
		Short: "List mountable topics from object storage",
		Long: `List topics that are available to mount from object storage.

This command displays topics that exist in object storage and can be mounted
to your Redpanda cluster. Each topic includes its location in object storage
and namespace information if applicable.`,
		Example: `
List all mountable topics:
  rpk cluster storage list-mountable
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]mountableTopicState{}); ok {
				out.Exit(h)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(p)

			var mountableTopics []rpadmin.MountableTopic
			if p.FromCloud {
				cl, err := createDataplaneClient(p)
				out.MaybeDieErr(err)

				resp, err := cl.CloudStorage.ListMountableTopics(cmd.Context(), connect.NewRequest(&dataplanev1alpha2.ListMountableTopicsRequest{}))
				out.MaybeDie(err, "unable to list mountable topics: %v", err)
				if resp != nil {
					mountableTopics = dataplaneToAdminMountableTopics(resp.Msg)
				}
			} else {
				adm, err := adminapi.NewClient(cmd.Context(), fs, p)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				response, err := adm.ListMountableTopics(cmd.Context())
				out.MaybeDie(err, "unable to list mountable topics: %v", err)
				mountableTopics = response.Topics
			}

			printDetailedListMountable(f, rpadminMountableTopicsToMountableTopicState(mountableTopics), os.Stdout)
		},
	}
	p.InstallFormatFlag(cmd)
	return cmd
}

func printDetailedListMountable(f config.OutFormatter, d []mountableTopicState, w io.Writer) {
	if isText, _, t, err := f.Format(d); !isText {
		out.MaybeDie(err, "unable to print in the requested format %q: %v", f.Kind, err)
		fmt.Fprintln(w, t)
		return
	}
	tw := out.NewTableTo(w, "Topic", "Namespace", "Location")
	defer tw.Flush()
	for _, m := range d {
		namespace := "kafka" // default namespace
		if m.Namespace != nil {
			namespace = *m.Namespace
		}
		tw.Print(m.Topic, namespace, m.TopicLocation)
	}
}

type mountableTopicState struct {
	Topic         string  `json:"topic" yaml:"topic"`
	Namespace     *string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	TopicLocation string  `json:"topic_location" yaml:"topic_location"`
}

func rpadminMountableTopicsToMountableTopicState(in []rpadmin.MountableTopic) []mountableTopicState {
	resp := make([]mountableTopicState, 0, len(in))
	for _, entry := range in {
		state := mountableTopicState{
			Topic:         entry.Topic,
			Namespace:     entry.Namespace,
			TopicLocation: entry.TopicLocation,
		}
		resp = append(resp, state)
	}
	return resp
}

func dataplaneToAdminMountableTopics(resp *dataplanev1alpha2.ListMountableTopicsResponse) []rpadmin.MountableTopic {
	var topics []rpadmin.MountableTopic
	if resp != nil {
		for _, topic := range resp.Topics {
			topics = append(topics, rpadmin.MountableTopic{
				TopicLocation: topic.TopicLocation,
				Topic:         topic.Name,
			})
		}
	}
	return topics
}
