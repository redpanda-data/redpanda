// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// Package connections deals with listing current connections in the cluster
package connections

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"

	adminv2 "buf.build/gen/go/redpandadata/core/protocolbuffers/go/redpanda/core/admin/v2"
	"connectrpc.com/connect"
	"github.com/docker/go-units"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/publicapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

const defaultPageSize = 20

var (
	minVersion            = redpanda.Version{Major: 25, Feature: 3}
	errInvalidStateFilter = errors.New("invalid state filter")
)

var stateOptionMap = map[string]adminv2.KafkaConnectionState{
	"OPEN":   adminv2.KafkaConnectionState_KAFKA_CONNECTION_STATE_OPEN,
	"CLOSED": adminv2.KafkaConnectionState_KAFKA_CONNECTION_STATE_CLOSED,
}

var tableHeaders = []string{
	"UID",
	"STATE",
	"USER",
	"CLIENT-ID",
	"IP:PORT",
	"NODE",
	"SHARD",
	"OPEN-TIME",
	"IDLE",
	"PROD-TPUT/SEC",
	"FETCH-TPUT/SEC",
	"REQS/MIN",
}

func getConnectionDuration(conn *adminv2.KafkaConnection) string {
	opened := conn.OpenTime.AsTime()
	closed := conn.CloseTime.AsTime()

	duration := closed.Sub(opened)
	if opened.After(closed) {
		duration = time.Since(opened)
	}

	return duration.Round(time.Second).String()
}

func writeConnectionRow(w *out.TabWriter, conn *Connection) {
	userString := conn.Authentication.UserPrincipal
	if conn.Authentication.State != "SUCCESS" {
		userString = "UNAUTHENTICATED"
	}

	w.Print(
		conn.UID,
		conn.State,
		userString,
		conn.Client.ID,
		fmt.Sprintf("%s:%d", conn.Client.IP, conn.Client.Port),
		conn.NodeID,
		conn.ShardID,
		conn.ConnectionDuration,
		conn.IdleDuration,
		units.HumanSize(float64(conn.RequestStatistics1m.ProduceBytes/60)),
		units.HumanSize(float64(conn.RequestStatistics1m.FetchBytes/60)),
		conn.RequestStatistics1m.RequestCount,
	)
}

func printConnectionListTable(connections []*Connection) string {
	if len(connections) <= 0 {
		return "No open connections found."
	}

	var buf bytes.Buffer
	writer := out.NewTableTo(&buf, tableHeaders...)

	for _, conn := range connections {
		writeConnectionRow(writer, conn)
	}

	writer.Flush()
	return buf.String()
}

type flagFilters struct {
	state                 string
	ipAddress             string
	clientID              string
	clientSoftwareName    string
	clientSoftwareVersion string
	groupID               string
	idleMs                int64
	user                  string
}

func (f *flagFilters) buildFilterClauses() ([]string, error) {
	clauses := []string{}

	if f.state != "" {
		val, ok := stateOptionMap[f.state]
		if !ok {
			return nil, fmt.Errorf("%w: %s", errInvalidStateFilter, f.state)
		}
		clauses = append(clauses, fmt.Sprintf("state = %s", val.String()))
	}

	// We have a series of string options we need to map to the upstream API filters
	for key, val := range map[string]string{
		"source.ip_address":                  f.ipAddress,
		"client_id":                          f.clientID,
		"client_software_name":               f.clientSoftwareName,
		"client_software_version":            f.clientSoftwareVersion,
		"group_id":                           f.groupID,
		"authentication_info.user_principal": f.user,
	} {
		if val != "" {
			clauses = append(clauses, fmt.Sprintf("%s = %q", key, val))
		}
	}

	if f.idleMs > 0 {
		clauses = append(clauses, fmt.Sprintf("idle_duration > %dms", f.idleMs))
	}
	return clauses, nil
}

func newConnectionList(fs afero.Fs, p *config.Params) *cobra.Command {
	var filters flagFilters
	var filterRaw string
	var orderBy string
	var limit int32

	cmd := &cobra.Command{
		Use:   "list",
		Short: "Display statistics about current kafka connections",
		Long: `Display statistics about current kafka connections.

This command displays a table of active and recently closed connections within the cluster.

Use filtering and sorting to identify the connections of the client applications that you are interested in. See --help for the list of filtering arguments and sorting arguments.

In addition to the filtering shorthand cli arguments (e.g.; --client-id, --state), you can also use the --filter-raw and --order-by arguments that take string expressions. To understand the syntax of these arguments, refer to the admin API docs of the filter and order-by fields of the ListKafkaConnections endpoint: https://docs.redpanda.com/api/doc/admin/version/11f41833-5783-4f1a-ad64-5957267abd52/operation/operation-redpanda-core-admin-v2-clusterservice-listkafkaconnections

By default only a subset of the per-connection data is printed. To see all of the available data, use --format=json.`,
		Example: `
List connections ordered by their recent produce throughput:
	rpk cluster connections list --order-by="recent_request_statistics.produce_bytes desc"

List connections ordered by their recent fetch throughput:
	rpk cluster connections list --order-by="recent_request_statistics.fetch_bytes desc"

List connections ordered by the time that they've been idle:
	rpk cluster connections list --order-by="idle_duration desc"

List connections ordered by those that have made the least requests:
	rpk cluster connections list --order-by="total_request_statistics.request_count asc"

List extended output for open connections in json format:
	rpk cluster connections list --format=json --state="OPEN"`,
		Args: cobra.ExactArgs(0),
		Run: func(cmd *cobra.Command, _ []string) {
			if h, ok := p.Formatter.Help([]Connection{}); ok {
				out.Exit(h)
			}

			prof, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitServerlessAdmin(prof)

			// Build the filters based on the current flag set
			filterClauses, err := filters.buildFilterClauses()
			out.MaybeDie(err, "invalid filters: %v", err)

			filterString := filterRaw
			if len(filterClauses) > 0 {
				filterString = strings.Join(filterClauses, " AND ")
			}

			req := adminv2.ListKafkaConnectionsRequest{
				Filter:   filterString,
				OrderBy:  orderBy,
				PageSize: limit,
			}

			var response *adminv2.ListKafkaConnectionsResponse
			if prof.CheckFromCloud() {
				cl, err := publicapi.DataplaneClientFromRpkProfile(prof)
				out.MaybeDie(err, "unable to initialize cloud API client: %v", err)

				resp, err := cl.Monitoring.ListKafkaConnections(cmd.Context(), &connect.Request[adminv2.ListKafkaConnectionsRequest]{Msg: &req})
				out.MaybeDie(err, "error listing connections: %v", err)

				response = resp.Msg
			} else {
				cl, err := adminapi.NewClient(cmd.Context(), fs, prof)
				out.MaybeDie(err, "unable to initialize admin client: %v", err)

				// Check that we're at least at the minimum version
				if !adminapi.HasMinimumVersion(cmd.Context(), cl, minVersion) {
					out.Die("rpk cluster connections list requires Redpanda version %s or later", minVersion.String())
				}

				resp, err := cl.ClusterService().ListKafkaConnections(cmd.Context(), &connect.Request[adminv2.ListKafkaConnectionsRequest]{Msg: &req})
				out.MaybeDie(err, "error listing connections: %v", err)

				response = resp.Msg
			}

			if p.Formatter.IsText() {
				conns := make([]*Connection, len(response.Connections))
				for i, conn := range response.Connections {
					conns[i] = parseConnection(conn)
				}
				fmt.Println(printConnectionListTable(conns))
			} else {
				_, _, output, err := p.Formatter.Format(response)
				out.MaybeDie(err, "unable to print in the required format %q: %v", p.Formatter.Kind, err)
				out.Exit(output)
			}
		},
	}

	// Set filtering flags
	fset := cmd.Flags()
	fset.StringVarP(&filters.state, "state", "s", "", "Filter results by state (OPEN, CLOSED)")
	fset.StringVar(&filters.ipAddress, "ip-address", "", "Filter results by the client ip address")
	fset.StringVar(&filters.clientID, "client-id", "", "Filter results by the client ID")
	fset.StringVar(&filters.clientSoftwareName, "client-software-name", "", "Filter results by the client software name")
	fset.StringVar(&filters.clientSoftwareVersion, "client-software-version", "", "Filter results by the client software version")
	fset.StringVarP(&filters.groupID, "group-id", "g", "", "Filter by client group ID")
	fset.Int64VarP(&filters.idleMs, "idle-ms", "i", 0, "Show connections idle for more than i milliseconds")
	fset.StringVarP(&filters.user, "user", "u", "", "Filter results by a specific user principal")
	fset.StringVar(&filterRaw, "filter-raw", "", "Filter connections based on a raw query (overrides other filters)")

	// TODO: add guardrails and define a proper field mapping
	fset.StringVar(&orderBy, "order-by", "", "Order the results by their values. See Examples above")

	// TODO: establish a limit
	fset.Int32Var(&limit, "limit", defaultPageSize, "Limit how many records can be returned")

	// Ensure --filters-raw can't be used with the other filters
	cmd.MarkFlagsMutuallyExclusive("filter-raw", "state")
	cmd.MarkFlagsMutuallyExclusive("filter-raw", "ip-address")
	cmd.MarkFlagsMutuallyExclusive("filter-raw", "client-id")
	cmd.MarkFlagsMutuallyExclusive("filter-raw", "client-software-name")
	cmd.MarkFlagsMutuallyExclusive("filter-raw", "client-software-version")
	cmd.MarkFlagsMutuallyExclusive("filter-raw", "group-id")
	cmd.MarkFlagsMutuallyExclusive("filter-raw", "idle-ms")
	cmd.MarkFlagsMutuallyExclusive("filter-raw", "user")

	p.InstallFormatFlag(cmd)
	return cmd
}
