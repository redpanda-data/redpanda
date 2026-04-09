package brokers

import (
	"fmt"
	"sort"
	"strings"

	"github.com/docker/go-units"
	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

type brokerResponse struct {
	NodeID     int                 `json:"node_id" yaml:"node_id"`
	Host       string              `json:"host,omitempty" yaml:"host,omitempty"`
	Port       *int                `json:"port,omitempty" yaml:"port,omitempty"`
	Rack       string              `json:"rack,omitempty" yaml:"rack,omitempty"`
	Cores      *int                `json:"cores,omitempty" yaml:"cores,omitempty"`
	Membership string              `json:"membership,omitempty" yaml:"membership,omitempty"`
	IsAlive    *bool               `json:"is_alive,omitempty" yaml:"is_alive,omitempty"`
	Version    string              `json:"version,omitempty" yaml:"version,omitempty"`
	UUID       string              `json:"uuid,omitempty" yaml:"uuid,omitempty"`
	DiskSpace  []diskSpaceResponse `json:"disk_space" yaml:"disk_space"`
}

type diskSpaceResponse struct {
	Path        string  `json:"path" yaml:"path"`
	Free        int64   `json:"free" yaml:"free"`
	Total       int64   `json:"total" yaml:"total"`
	UsedPercent float64 `json:"used_percent" yaml:"used_percent"`
}

func newListCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		decom    bool
		detailed bool
	)
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List the brokers in your cluster",
		Long: `List the brokers in your cluster.

This command lists all brokers in the cluster, active and inactive, unless they
have been decommissioned. Using the "--include-decommissioned" flag, it lists
decommissioned brokers with associated UUIDs too.

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

NOTE: The UUID column is hidden when the cluster doesn't expose the UUID in the
Admin API, or the API call fails to retrieve UUIDs.

Use the --detailed flag to display an additional DISK SPACE section showing
per-mount-path disk usage for each broker. The disk space table has the
following columns:

  NODE-ID  Broker node ID
  PATH     Mount path on the broker
  FREE     Free disk space (human-readable)
  TOTAL    Total disk space (human-readable)
  USED%    Percentage of disk space used

When using --format json or --format yaml, disk space information is always
included regardless of the --detailed flag.
`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			f := p.Formatter
			if h, ok := f.Help([]brokerResponse{}); ok {
				out.Exit(h)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			bs, err := cl.Brokers(cmd.Context())
			out.MaybeDie(err, "unable to request brokers: %v", err)

			idUUIDMapping, err := cl.GetBrokerUuids(cmd.Context())
			if err != nil {
				fmt.Fprintf(cmd.ErrOrStderr(), "unable to retrieve node UUIDs: %v\n", err)
			}

			// Build the response struct with raw values; formatting
			// (e.g. "-" for empty fields) is applied only in text output.
			resp := make([]brokerResponse, 0, len(bs))
			for _, b := range bs {
				version, _ := redpanda.VersionFromString(b.Version)
				ds := make([]diskSpaceResponse, 0, len(b.DiskSpace))
				for _, d := range b.DiskSpace {
					var usedPct float64
					if d.Total > 0 {
						usedPct = float64(d.Total-d.Free) / float64(d.Total) * 100
					}
					ds = append(ds, diskSpaceResponse{
						Path:        d.Path,
						Free:        int64(d.Free),
						Total:       int64(d.Total),
						UsedPercent: usedPct,
					})
				}
				alive := *b.IsAlive
				port := b.InternalRPCPort
				cores := b.NumCores
				br := brokerResponse{
					NodeID:     b.NodeID,
					Host:       b.InternalRPCAddress,
					Port:       &port,
					Rack:       b.Rack,
					Cores:      &cores,
					Membership: string(b.MembershipStatus),
					IsAlive:    &alive,
					Version:    version.String(),
					DiskSpace:  ds,
				}
				if idUUIDMapping != nil {
					br.UUID = mapUUID(b.NodeID, idUUIDMapping)
				}
				resp = append(resp, br)
			}

			if decom && idUUIDMapping != nil {
				for _, b := range extractDecomNodes(bs, idUUIDMapping) {
					resp = append(resp, brokerResponse{
						NodeID:    b.NodeID,
						UUID:      b.UUID,
						DiskSpace: []diskSpaceResponse{},
					})
				}
			}

			if isText, _, s, err := f.Format(resp); !isText {
				out.MaybeDie(err, "unable to print in the required format %q: %v", f.Kind, err)
				fmt.Println(s)
				return
			}

			printBrokerList(resp, idUUIDMapping != nil, detailed)
		},
	}
	cmd.Flags().BoolVarP(&decom, "include-decommissioned", "d", false, "If true, include decommissioned brokers")
	cmd.Flags().BoolVar(&detailed, "detailed", false, "If true, include per-broker disk space information")
	p.InstallFormatFlag(cmd)
	return cmd
}

func printBrokerList(resp []brokerResponse, hasUUIDs, detailed bool) {
	sort.Slice(resp, func(i, j int) bool { return resp[i].NodeID < resp[j].NodeID })

	const (
		secBrokers   = "Brokers"
		secDiskSpace = "Disk space"
	)
	sections := out.NewMaybeHeaderSections(
		out.ConditionalSectionHeaders(map[string]bool{
			secBrokers:   true,
			secDiskSpace: detailed,
		})...,
	)

	sections.Add(secBrokers, func() {
		headers := []string{"ID", "Host", "Port", "Rack", "Cores", "Membership", "Is-Alive", "Version"}
		if hasUUIDs {
			headers = append(headers, "UUID")
		}
		tw := out.NewTable(headers...)
		defer tw.Flush()
		for _, b := range resp {
			row := []interface{}{
				b.NodeID,
				fmtString(b.Host),
				fmtIntPtr(b.Port),
				fmtString(b.Rack),
				fmtIntPtr(b.Cores),
				fmtString(b.Membership),
				fmtBoolPtr(b.IsAlive),
				formatOutput(b.Version),
			}
			if hasUUIDs {
				row = append(row, b.UUID)
			}
			tw.Print(row...)
		}
	})

	sections.Add(secDiskSpace, func() {
		tw := out.NewTable("Node-ID", "Path", "Free", "Total", "Used%")
		defer tw.Flush()
		for _, b := range resp {
			for _, d := range b.DiskSpace {
				tw.Print(b.NodeID, d.Path, units.HumanSize(float64(d.Free)), units.HumanSize(float64(d.Total)), fmt.Sprintf("%.1f%%", d.UsedPercent))
			}
		}
	})
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

func fmtString(s string) interface{} {
	if s == "" {
		return "-"
	}
	return s
}

func fmtIntPtr(p *int) interface{} {
	if p == nil {
		return "-"
	}
	return *p
}

func fmtBoolPtr(p *bool) interface{} {
	if p == nil {
		return "-"
	}
	return *p
}
