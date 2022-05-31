// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package debug

import (
	"fmt"
	"time"

	"github.com/docker/go-units"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/api/admin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cli/cmd/common"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

// Use the same date specs as journalctl (see `man journalctl`).
const timeHelpText = `(journalctl date format, e.g. YYYY-MM-DD)`

func newBundleCommand(fs afero.Fs) *cobra.Command {
	var (
		configFile string

		brokers   []string
		user      string
		password  string
		mechanism string
		enableTLS bool
		certFile  string
		keyFile   string
		caFile    string

		adminURL       string
		adminEnableTLS bool
		adminCertFile  string
		adminKeyFile   string
		adminCAFile    string

		logsSince     string
		logsUntil     string
		logsSizeLimit string

		timeout time.Duration
	)
	command := &cobra.Command{
		Use:   "bundle",
		Short: "Collect environment data and create a bundle file for the Redpanda Data support team to inspect.",
		Long:  bundleHelpText,
		Run: func(cmd *cobra.Command, args []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			admin, err := admin.NewClient(fs, cfg)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			cl, err := kafka.NewFranzClient(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			logsLimit, err := units.FromHumanSize(logsSizeLimit)
			out.MaybeDie(err, "unable to parse --logs-size-limit: %v", err)

			err = executeBundle(fs, cfg, cl, admin, logsSince, logsUntil, int(logsLimit), timeout)
			out.MaybeDie(err, "unable to create bundle: %v", err)
		},
	}
	command.Flags().StringVar(
		&adminURL,
		"admin-url",
		"",
		"The address to the broker's admin API. Defaults to the one in the config file.",
	)
	command.Flags().DurationVar(
		&timeout,
		"timeout",
		10*time.Second,
		"How long to wait for child commands to execute (e.g. '30s', '1.5m')",
	)
	command.Flags().StringVar(
		&logsSince,
		"logs-since",
		"",
		fmt.Sprintf(`Include log entries on or newer than the specified date. %s`, timeHelpText),
	)
	command.Flags().StringVar(
		&logsUntil,
		"logs-until",
		"",
		fmt.Sprintf(`Include log entries on or older than the specified date. %s`, timeHelpText),
	)
	command.Flags().StringVar(
		&logsSizeLimit,
		"logs-size-limit",
		"100MiB",
		"Read the logs until the given size is reached. Multipliers are also supported, e.g. 3MB, 1GiB.",
	)

	common.AddKafkaFlags(
		command,
		&configFile,
		&user,
		&password,
		&mechanism,
		&enableTLS,
		&certFile,
		&keyFile,
		&caFile,
		&brokers,
	)
	common.AddAdminAPITLSFlags(command,
		&adminEnableTLS,
		&adminCertFile,
		&adminKeyFile,
		&adminCAFile,
	)

	return command
}

const bundleHelpText = `'rpk debug bundle' collects environment data that can help debug and diagnose
issues with a redpanda cluster, a broker, or the machine it's running on. It
then bundles the collected data into a zip file.

The following are the data sources that are bundled in the compressed file:

 - Kafka metadata: Broker configs, topic configs, start/committed/end offsets,
   groups, group commits.

 - Data directory structure: A file describing the data directory's contents.

 - redpanda configuration: The redpanda configuration file (redpanda.yaml;
   SASL credentials are stripped).

 - /proc/cpuinfo: CPU information like make, core count, cache, frequency.

 - /proc/interrupts: IRQ distribution across CPU cores.

 - Resource usage data: CPU usage percentage, free memory available for the
   redpanda process.

 - Clock drift: The ntp clock delta (using pool.ntp.org as a reference) & round
   trip time.

 - Kernel logs: The kernel logs ring buffer (syslog).

 - Broker metrics: The local broker's Prometheus metrics, fetched through its
   admin API.

 - DNS: The DNS info as reported by 'dig', using the hosts in
   /etc/resolv.conf.

 - Disk usage: The disk usage for the data directory, as output by 'du'.

 - redpanda logs: The redpanda logs written to journald. If --logs-since or
   --logs-until are passed, then only the logs within the resulting time frame
   will be included.

 - Socket info: The active sockets data output by 'ss'.

 - Running process info: As reported by 'top'.

 - Virtual memory stats: As reported by 'vmstat'.

 - Network config: As reported by 'ip addr'.

 - lspci: List the PCI buses and the devices connected to them.

 - dmidecode: The DMI table contents. Only included if this command is run
   as root.
`
