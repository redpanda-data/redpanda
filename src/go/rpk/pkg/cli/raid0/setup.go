// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package raid0

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/raid0"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

func newSetupCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var (
		devices            string
		chunkSizeKiB       uint32
		blockSizeKiB       uint16
		mountPath          string
		disableMountOnBoot bool
		mountOptions       string
		timeout            time.Duration
	)
	cmd := &cobra.Command{
		Use:   "setup /dev/md0",
		Short: "Setup a RAID0 device for Redpanda's data",
		Long: `Setup a RAID0 device for Redpanda's data.

This command requires root privileges to create, format and mount a Linux RAID0
device, intended to be used as data directory with Redpanda.

The RAID0 is optimized by default for cloud environments with ephemeral SSD disks.
Any data in the RAID member devices will be deleted, all blocks will discarded and
superblocks erased.

The setup includes adding the RAID device definition into mdadm.conf(5) and
updating initramfs so it is re-assembled correctly on boot, either in the same
machine or a different machine (in case of host migrations).

The RAID0 device is formatted with XFS and mounted using a systemd mount unit
placed under /etc/systemd/system and enabled to mount the RAID device on boot.
		`,
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) == 0 {
				cmd.Help()
				out.Die("\n! a nonexistent device path is required. Ex: /dev/md0")
			}

			if os.Geteuid() != 0 {
				out.Die("! this command must be run as root")
			}

			devicePath := args[0]
			memberDevices := strings.Split(devices, ",")

			r := raid0.NewRedpandaRAID(fs, zap.L(),
				raid0.DevicePath(devicePath),
				raid0.MemberDevicePaths(memberDevices),
				raid0.MountPath(mountPath),
				raid0.DisableMountOnBoot(),
				raid0.BlockSizeBytes(blockSizeKiB*1024),
				raid0.ChunkSizeKiB(chunkSizeKiB),
				raid0.MountOptions(mountOptions),
			)

			ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
			fmt.Printf("Setting up RAID0 in %q...\n", devicePath)
			if err := r.Setup(ctx); err != nil {
				cancel()
				out.MaybeDieErr(err)
			}
			cancel()
			fmt.Println("RAID0 device was successfully created")
		},
	}

	cmd.MarkFlagRequired("devices")

	flags := cmd.Flags()
	flags.StringVarP(&devices, "devices", "d", "", "Disk devices to use to create the RAID 0 device")
	flags.Uint32VarP(&chunkSizeKiB, "chunk-size-kib", "c", 16, "RAID 0 chunk size or strip depth, in kibibytes (KiB)")
	flags.Uint16VarP(&blockSizeKiB, "block-size-kib", "b", 4, "XFS block size in kibibytes (KiB)")
	flags.StringVarP(&mountPath, "mount-path", "m", "/var/lib/redpanda", "Destination mount path")
	flags.StringVarP(&mountOptions, "mount-options", "o", "defaults,noatime,nodiratime", "XFS mount options")
	flags.DurationVar(&timeout, "timeout", 1*time.Minute, "The maximum time to wait for the command to finish")
	flags.BoolVar(&disableMountOnBoot, "disable-mount-on-boot", false, "Disables configuring the systemd mount unit and starting it on boot")
	return cmd
}
