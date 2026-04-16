/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once

#include <seastar/core/sstring.hh>

#include <base/seastarx.h>
#include <sys/types.h>

namespace utils {

/**
 * Utility class for resolving file system paths to their underlying
 * block device names. This is used to determine which partitions or
 * physical disk a path resides on.
 */
class device_resolver {
public:
    struct resolved_device {
        ss::sstring name; // device name (e.g., "sda3", "nvme0n1p1")
        dev_t dev_id;     // device ID from stat(2) st_dev
    };

    /**
     * Resolve a filesystem path to its device name and device ID.
     *
     * The device name is the name of the block device that stat(2)
     * returns for the path. This will generally be a partition, not
     * the whole disk (unless you didn't partition your disk).
     *
     * The device name is NOT prefixed with /dev/.
     *
     * @param path Directory path to resolve
     * @return Device name and dev_t
     * @throws std::runtime_error if the path cannot be resolved to a
     *         block device (e.g. inaccessible path, overlay/tmpfs
     *         filesystem, loop device)
     *
     * Examples:
     * - "/var/lib/redpanda" on /dev/sda3 -> {"sda3", <dev_t>}
     * - "/mnt/vectorized" on /dev/nvme0n1p1 -> {"nvme0n1p1", <dev_t>}
     * - Path on whole-disk filesystem -> {"sda", <dev_t>}
     */
    static resolved_device device_for_path(const ss::sstring& path);

    /**
     * Strip partition number from a device name to get the base device.
     *
     * This is useful to find the underlying disk for a partition.
     *
     * This is a pure string operation matching common device patterns and
     * removing the partition part.
     *
     * If this matching fails, it returns the string unchanged.
     *
     * @param device Device name possibly with partition (e.g., "sda3",
     * "nvme0n1p2")
     * @return Base device name (e.g., "sda", "nvme0n1")
     *
     * Examples:
     * - "sda3" -> "sda"
     * - "nvme0n1p2" -> "nvme0n1"
     * - "vda1" -> "vda"
     * - "sda" -> "sda" (already base device)
     */
    static ss::sstring get_base_device(const ss::sstring& device) noexcept;
};

} // namespace utils
