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

#include "utils/device_utils.h"

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/sysmacros.h>

namespace utils {

TEST(DeviceUtils, GetBaseDeviceTraditional) {
    // Test traditional disk partition stripping
    EXPECT_EQ(device_resolver::get_base_device("sda1"), "sda");
    EXPECT_EQ(device_resolver::get_base_device("sda2"), "sda");
    EXPECT_EQ(device_resolver::get_base_device("sda10"), "sda");
    EXPECT_EQ(device_resolver::get_base_device("sda"), "sda");

    EXPECT_EQ(device_resolver::get_base_device("vda1"), "vda");
    EXPECT_EQ(device_resolver::get_base_device("vda"), "vda");

    EXPECT_EQ(device_resolver::get_base_device("xvda1"), "xvda");
    EXPECT_EQ(device_resolver::get_base_device("xvda"), "xvda");

    EXPECT_EQ(device_resolver::get_base_device("hda3"), "hda");
    EXPECT_EQ(device_resolver::get_base_device("hda"), "hda");
}

TEST(DeviceUtils, GetBaseDeviceNVMe) {
    // Test NVMe device partition stripping
    EXPECT_EQ(device_resolver::get_base_device("nvme0n1p1"), "nvme0n1");
    EXPECT_EQ(device_resolver::get_base_device("nvme0n1p2"), "nvme0n1");
    EXPECT_EQ(device_resolver::get_base_device("nvme0n1p10"), "nvme0n1");
    EXPECT_EQ(device_resolver::get_base_device("nvme0n1"), "nvme0n1");

    EXPECT_EQ(device_resolver::get_base_device("nvme1n1p1"), "nvme1n1");
    EXPECT_EQ(device_resolver::get_base_device("nvme10n1p5"), "nvme10n1");
}

TEST(DeviceUtils, GetBaseDeviceEdgeCases) {
    // Test edge cases
    EXPECT_EQ(device_resolver::get_base_device(""), "");

    // pmem (persistent memory) devices use 'p' prefix for partitions
    EXPECT_EQ(device_resolver::get_base_device("pmem0"), "pmem0");
    EXPECT_EQ(device_resolver::get_base_device("pmem0p1"), "pmem0");

    // dm devices, shouldn't strip
    EXPECT_EQ(device_resolver::get_base_device("dm-0"), "dm-0");
    EXPECT_EQ(device_resolver::get_base_device("dm-10"), "dm-10");
    EXPECT_EQ(device_resolver::get_base_device("dm0"), "dm0");

    // MD RAID partitions: md0p17 -> md0
    EXPECT_EQ(device_resolver::get_base_device("md0p1"), "md0");
    EXPECT_EQ(device_resolver::get_base_device("md0p17"), "md0");
    EXPECT_EQ(device_resolver::get_base_device("md127p3"), "md127");

    // MD RAID base devices, shouldn't strip
    EXPECT_EQ(device_resolver::get_base_device("md0"), "md0");
    EXPECT_EQ(device_resolver::get_base_device("loop0"), "loop0");
    EXPECT_EQ(device_resolver::get_base_device("sr0"), "sr0");

    // no match, pass unchanged
    EXPECT_EQ(device_resolver::get_base_device("foobar"), "foobar");
}

TEST(DeviceUtils, GetBaseDeviceNoPartition) {
    // Test devices that are already base devices
    EXPECT_EQ(device_resolver::get_base_device("sda"), "sda");
    EXPECT_EQ(device_resolver::get_base_device("nvme0n1"), "nvme0n1");
    EXPECT_EQ(device_resolver::get_base_device("vda"), "vda");
}

TEST(DeviceUtils, GetBaseDeviceMultipleDigits) {
    // Test devices with multiple trailing digits
    EXPECT_EQ(device_resolver::get_base_device("sda123"), "sda");
    EXPECT_EQ(device_resolver::get_base_device("nvme0n1p999"), "nvme0n1");
}

TEST(DeviceUtils, DeviceForPathRootDirectory) {
    // In containers the root filesystem may be on an overlay (major 0)
    // with no /sys/dev/block entry, so skip when that's the case.
    struct stat st{};
    ASSERT_EQ(stat("/", &st), 0);
    if (major(st.st_dev) == 0) {
        GTEST_SKIP() << "root device has major 0 (likely overlay/container)";
    }

    auto resolved = device_resolver::device_for_path("/");
    EXPECT_FALSE(resolved.name.empty());
    EXPECT_NE(resolved.dev_id, 0);

    // The partition device should start with its base device name
    // (e.g. sda3 starts with sda, nvme0n1p5 starts with nvme0n1).
    auto base = device_resolver::get_base_device(resolved.name);
    EXPECT_TRUE(resolved.name.starts_with(base))
      << "partition device '" << resolved.name << "' should start with base '"
      << base << "'";
}

TEST(DeviceUtils, DeviceForPathNonExistent) {
    EXPECT_THROW(
      device_resolver::device_for_path("/this/path/does/not/exist"),
      std::runtime_error);
}

TEST(DeviceUtils, DeviceForPathBaseDeviceConsistency) {
    struct stat st{};
    ASSERT_EQ(stat("/", &st), 0);
    if (major(st.st_dev) == 0) {
        GTEST_SKIP() << "root device has major 0 (likely overlay/container)";
    }

    // get_base_device(device_for_path(path)) should yield a valid base device
    auto resolved = device_resolver::device_for_path("/");
    auto base = device_resolver::get_base_device(resolved.name);
    EXPECT_FALSE(base.empty());

    // Common root devices (one of these should match)
    bool is_common_device = base.starts_with("sd") || base.starts_with("nvme")
                            || base.starts_with("vd") || base.starts_with("xvd")
                            || base.starts_with("hd") || base.starts_with("dm-")
                            || base.starts_with("md");
    EXPECT_TRUE(is_common_device) << "Unexpected root device: " << base;
}

} // namespace utils
