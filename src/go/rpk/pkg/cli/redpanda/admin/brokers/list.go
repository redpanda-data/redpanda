package brokers

import (
	"fmt"
	"strings"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var decom bool
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List the brokers in your cluster",
		Long: `List the brokers in your cluster.

This command lists all brokers in the cluster, active and inactive, unless they have been decommissioned.
Using the "--include-decommissioned" flag, it lists decommissioned brokers with associated UUIDs too.

The output table contains the following columns:

ID               Node ID, an exclusive identifier for a broker
HOST             Internal RPC address for communication between brokers
PORT             Internal RPC port for communication between brokers
RACK             Assigned rack ID
CORES            Number of cores (shards) on a broker
MEMBERSHIP       Whether a broker is decommissioned or not
IS-ALIVE         Whether a broker is alive or offline
VERSION          Broker version
UUID (Optional)  Additional exclusive identifier for a broker

NOTE: The UUID column is hidden when the cluster doesn't expose the UUID in the Admin API, or the API call fails to retrieve UUIDs.
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			bs, err := cl.Brokers(cmd.Context())
			out.MaybeDie(err, "unable to request brokers: %v", err)

			headers := []string{"ID", "Host", "Port", "Rack", "Cores", "Membership", "Is-Alive", "Version"}

			args := func(b *rpadmin.Broker) []interface{} {
				version, _ := redpanda.VersionFromString(b.Version)
				ret := []interface{}{b.NodeID, b.InternalRPCAddress, b.InternalRPCPort, formatOutput(b.Rack), b.NumCores, b.MembershipStatus, *b.IsAlive, formatOutput(version.String())}
				return ret
			}

			idUUIDMapping, err := cl.GetBrokerUuids(cmd.Context())
			if err != nil {
				fmt.Printf("unable to retrieve node UUIDs: %v", err)
			}
			if idUUIDMapping != nil {
				headers = append(headers, "UUID")
				org := args
				args = func(b *rpadmin.Broker) []interface{} {
					return append(org(b), mapUUID(b.NodeID, idUUIDMapping))
				}
			}

			tw := out.NewTable(headers...)
			defer tw.Flush()
			for _, b := range bs {
				tw.Print(args(&b)...)
			}

			if decom && idUUIDMapping != nil {
				decomNodes := extractDecomNodes(bs, idUUIDMapping)
				for _, b := range decomNodes {
					tw.Print(b.NodeID, "-", "-", "-", "-", "-", "-", "-", b.UUID)
				}
			}
		},
	}
	cmd.Flags().BoolVarP(&decom, "include-decommissioned", "d", false, "If true, include decommissioned brokers")
	return cmd
}

// mapUUID returns a UUID from "mapping" which node ID maps to "nodeID".
func mapUUID(nodeID int, mapping []rpadmin.BrokerUuids) string {
	var UUIDs []string
	for _, node := range mapping {
		if nodeID == node.NodeID {
			UUIDs = append(UUIDs, node.UUID)
		}
	}
	if len(UUIDs) == 0 {
		return "-"
	}
	return strings.Join(UUIDs, ", ")
}

// extractDecomNodes compares and returns nodes in brokerUUIDs (with UUIDs) not in brokers.
func extractDecomNodes(brokers []rpadmin.Broker, brokerUUIDs []rpadmin.BrokerUuids) []rpadmin.BrokerUuids {
	activeNodeMap := make(map[int]bool)

	for _, br := range brokers {
		activeNodeMap[br.NodeID] = true
	}

	var decomNodes []rpadmin.BrokerUuids
	for _, bu := range brokerUUIDs {
		if !activeNodeMap[bu.NodeID] {
			decomNodes = append(decomNodes, rpadmin.BrokerUuids{
				NodeID: bu.NodeID,
				UUID:   bu.UUID,
			})
		}
	}

	return decomNodes
}

func formatOutput(s string) string {
	if s == "" || s == "0.0.0" {
		return "-"
	}
	return s
}
