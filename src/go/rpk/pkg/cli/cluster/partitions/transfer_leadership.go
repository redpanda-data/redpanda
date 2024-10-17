// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package partitions

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/redpanda-data/common-go/rpadmin"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

func newTransferLeaderCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var partitionArg string

	cmd := &cobra.Command{
		Use:   "transfer-leadership",
		Short: "Transfer partition leadership between brokers",
		Long: `Transfer partition leadership between brokers.

This command allows you to transfer partition leadership.
You can transfer only one partition leader at a time.

To transfer partition leadership, use the following syntax:
	rpk cluster partitions transfer-leadership foo --partition 0:2

Here, the command transfers leadership for the partition "kafka/foo/0"
to broker 2. By default, it assumes the "kafka" namespace, but you can specify
an internal namespace using the "{namespace}/" prefix.

Here is an equivalent command using different syntax:
	rpk cluster partitions transfer-leadership --partition foo/0:2

Warning: Redpanda tries to balance leadership distribution across brokers by default.
If the distribution of leaders becomes uneven as a result of transferring leadership
across brokers, the cluster may move leadership back to the original
brokers automatically.
`,

		Args: cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, topicArg []string) {
			f := p.Formatter
			if h, ok := f.Help([]string{}); ok {
				out.Exit(h)
			}

			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			cl, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			if len(topicArg) > 0 { // foo -p 0:1
				_, _, partition, target, err := extractNTPTarget(topicArg[0], partitionArg)
				out.MaybeDie(err, "failed to extract topic/partition: %s\n", err)

				ns, topic := formatNT(topicArg[0])

				partDetails, err := cl.GetPartition(cmd.Context(), ns, topic, partition)
				out.MaybeDie(err, "failed to get partition details: %s\n", err)

				source := partDetails.LeaderID

				err = transferLeadership(cmd.Context(), fs, p, cl, source, ns, topic, partition, target)
				if err != nil {
					fmt.Printf("failed to transfer the partition leadership: %v\n", err)
				} else {
					fmt.Println("Successfully began the partition leadership transfer(s).\n\nCheck the new leader assignment with 'rpk topic describe -p TOPIC'.")
				}
			} else { // -p foo/0:1
				ns, topic, partition, target, err := extractNTPTarget("", partitionArg)
				out.MaybeDie(err, "failed to extract topic/partition: %s\n", err)

				partDetails, err := cl.GetPartition(cmd.Context(), ns, topic, partition)
				out.MaybeDie(err, "failed to get partition details: %s\n", err)

				source := partDetails.LeaderID

				err = transferLeadership(cmd.Context(), fs, p, cl, source, ns, topic, partition, target)
				if err != nil {
					fmt.Printf("failed to transfer the partition leader: %v\n", err)
				} else {
					fmt.Println("Successfully began the partition leadership transfer(s).\n\nCheck the new leader assignment with 'rpk topic describe -p TOPIC'.")
				}
			}
		},
	}
	cmd.Flags().StringVarP(&partitionArg, "partition", "p", "", "Topic-partition to transfer leadership and new leader location")
	p.InstallFormatFlag(cmd)
	return cmd
}

// extractNTPTarget parses the partition flag with format; foo/0:1 or 0:1
// and returns namespace', 'topic', 'partition', and 'target node' separately.
func extractNTPTarget(topic string, ntp string) (ns string, t string, p int, target string, err error) {
	ntpReOnce.Do(func() {
		ntpRe = regexp.MustCompile(`^((?:[^:]+/)?\d+):(\d+)$`)
	})
	m := ntpRe.FindStringSubmatch(ntp)
	if len(m) == 0 {
		return "", "", -1, "", fmt.Errorf("invalid format for %s", ntp)
	}
	beforeColon := m[1]
	target = m[2]
	if topic != "" {
		p, err = strconv.Atoi(beforeColon)
		if err != nil {
			return "", "", -1, "", fmt.Errorf("%s", err)
		}
	} else if n := strings.Split(beforeColon, "/"); len(n) == 3 {
		ns = n[0]
		t = n[1]
		p, err = strconv.Atoi(n[2])
		if err != nil {
			return "", "", -1, "", fmt.Errorf("%s", err)
		}
	} else if len(n) == 2 {
		ns = "kafka"
		t = n[0]
		p, err = strconv.Atoi(n[1])
		if err != nil {
			return "", "", -1, "", fmt.Errorf("%s", err)
		}
	} else {
		return "", "", -1, "", fmt.Errorf("invalid format for %s", ntp)
	}
	return ns, t, p, target, nil
}

// formatNT parse a given '(namespace)/topic' string
// and return 'namespace' and 'topic' separately.
func formatNT(t string) (ns string, topic string) {
	if nt := strings.Split(t, "/"); len(nt) == 1 {
		ns = "kafka"
		topic = nt[0]
	} else {
		ns = nt[0]
		topic = nt[1]
	}
	return
}

// transferLeadership creates a host client that talks to the broker with the
// given ID and calls the TransferLeadership endpoint.
func transferLeadership(ctx context.Context, fs afero.Fs, p *config.RpkProfile, admCl *rpadmin.AdminAPI, id int, ns, topic string, partition int, target string) error {
	brokerURL, err := admCl.BrokerIDToURL(ctx, id)
	if err != nil {
		return fmt.Errorf("unable to get broker URL: %v", err)
	}
	a := &p.AdminAPI
	addrs := a.Addresses
	tc, err := a.TLS.Config(fs)
	if err != nil {
		return fmt.Errorf("unable to create admin api tls config: %v", err)
	}
	auth, err := adminapi.GetAuth(p)
	if err != nil {
		return fmt.Errorf("unable to get auth mechanism from loaded rpk profile: %v", err)
	}
	cl, err := rpadmin.NewHostClient(addrs, tc, auth, false, brokerURL)
	if err != nil {
		return fmt.Errorf("unable to create Admin API client for node with ID %v: %v", id, err)
	}
	return cl.TransferLeadership(ctx, ns, topic, partition, target)
}
