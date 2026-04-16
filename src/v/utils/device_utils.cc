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

#include "ssx/sformat.h"

#include <seastar/core/sstring.hh>

#include <sys/stat.h>
#include <sys/sysmacros.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <regex>
#include <stdexcept>

namespace utils {

device_resolver::resolved_device
device_resolver::device_for_path(const ss::sstring& path) {
    struct stat st{};
    if (stat(path.c_str(), &st) != 0) {
        throw std::runtime_error(
          ssx::sformat("stat('{}') failed: {}", path, strerror(errno)));
    }

    // st_dev is the device ID of the device containing the file
    dev_t dev = st.st_dev;
    unsigned int maj = major(dev);
    unsigned int min = minor(dev);

    // Resolve device via /sys/dev/block/major:minor symlink
    auto symlink_path = ssx::sformat("/sys/dev/block/{}:{}", maj, min);

    // read_symlink handles buffer management and throws
    // std::filesystem::filesystem_error on failure
    auto target = std::filesystem::read_symlink(symlink_path.c_str());
    auto device_name = target.filename().string();

    if (device_name.empty()) {
        throw std::runtime_error(
          ssx::sformat(
            "unexpected sysfs symlink format for dev {}:{}: '{}'",
            maj,
            min,
            target.string()));
    }

    return {.name = ss::sstring{device_name}, .dev_id = dev};
}

ss::sstring
device_resolver::get_base_device(const ss::sstring& device) noexcept {
    if (device.empty()) {
        return device;
    }

    // Define regex patterns for different device types
    // NVMe: nvme0n1p1 -> nvme0n1 (capture: nvme<controller>n<namespace>)
    static const std::regex nvme_pattern(R"(^(nvme\d+n\d+)p\d+$)");

    // MMC/SD: mmcblk0p1 -> mmcblk0 (capture: mmcblk<number>)
    static const std::regex mmc_pattern(R"(^(mmcblk\d+)p\d+$)");

    // MD RAID: md0p1 -> md0, md127p3 -> md127 (capture: md<number>)
    static const std::regex md_pattern(R"(^(md\d+)p\d+$)");

    // PMEM: pmem0p1 -> pmem0 (capture: pmem<number>)
    static const std::regex pmem_pattern(R"(^(pmem\d+)p\d+$)");

    // Traditional: sda1 -> sda, vda3 -> vda, xvda2 -> xvda, hdb5 -> hdb
    // Only match known partitioned device prefixes to avoid stripping
    // digits from non-partition base devices like md0, loop0, or dm-0.
    static const std::regex traditional_pattern(
      R"(^((sd|vd|xvd|hd)[a-z]+)\d+$)");

    std::string device_str{device};
    std::smatch match;

    // Try each pattern and return the captured base device name
    if (
      std::regex_match(device_str, match, nvme_pattern)
      || std::regex_match(device_str, match, md_pattern)
      || std::regex_match(device_str, match, mmc_pattern)
      || std::regex_match(device_str, match, pmem_pattern)
      || std::regex_match(device_str, match, traditional_pattern)) {
        return {match[1].str()};
    }

    // No partition found, return as-is
    return device;
}

} // namespace utils
