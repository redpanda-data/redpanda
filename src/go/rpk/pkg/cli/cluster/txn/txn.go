// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package txn

import (
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
)

const rfc3339Milli = "2006-01-02T15:04:05.999Z"

func NewCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "txn",
		Aliases: []string{"transaction"},
		Short:   "Information about transactions and transactional producers",
		Long: `Information about transactions and transactional producers.

Transactions allow producing, or consume-modifying-producing, to Redpanda.
The consume-modify-produce loop is also referred to as EOS (exactly once
semantics). Transactions involve a lot of technical complexity that is largely
hidden within clients. This command space helps shed a light on what is
actually happening in clients and brokers while transactions are in use.

TRANSACTIONAL ID

The transactional ID is the string you define in clients when actually using
transactions.

PRODUCER ID & EPOCH

The producer ID is generated within clients when you transactionally produce.
The producer ID is a number that maps to your transactional ID, allowing
requests to be smaller when producing, and allowing some optimizations within
brokers when managing transactions.

Some clients expose the producer ID, allowing you to track the transactional ID
that a producer ID maps to. If possible, it is recommended to monitor the
producer ID used in your applications.

The producer epoch is a number that somewhat "counts" the number of times your
transaction has been initialized or expired. If you have one client that uses
a transactional ID, it may receive producer ID 3 epoch 0. Another client that
uses that same transactional ID will receive producer ID 3 epoch 1. If the
client starts a transaction but does not finish it in time, the cluster will
internally bump the epoch to 2. The epoch allows the cluster to "fence"
clients: if a client attempts to use a producer ID with an old epoch, the
cluster will reject the client's produce request as stale.

TRANSACTION STATE

The state of a transaction indicates what is currently happening with a
transaction. A high level overview of transactional states:

  * Empty: the transactional ID is ready but there are no partitions
           nor groups added to it -- there is no active transaction
  * Ongoing: the transactional ID is being used in a began transaction
  * PrepareCommit: a commit is in progress
  * PrepareAbort: an abort is in progress
  * PrepareEpochFence: the transactional ID is timing out
  * Dead: the transactional ID has expired and/or is not in use

LAST STABLE OFFSET

The last stable offset is the offset at which a transaction has begun and
clients cannot consume past, if the client is configured to read only committed
offsets. The last stable offset can be seen when describing active transactional
producers by looking for the earliest transaction start offset per partition.
`,
	}
	p.InstallKafkaFlags(cmd)

	cmd.AddCommand(
		newDescribeCommand(fs, p),
		newDescribeProducersCommand(fs, p),
		newListCommand(fs, p),
	)
	p.InstallFormatFlag(cmd)
	return cmd
}
