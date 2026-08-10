/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "datalake/parquet_write_config.h"

#include "base/vlog.h"
#include "datalake/logger.h"
#include "strings/string_switch.h"

#include <algorithm>
#include <cctype>

namespace datalake {

namespace {
constexpr std::string_view codec_key = "write.parquet.compression-codec";
} // namespace

parquet_write_config parquet_write_config::from_properties(
  const std::optional<iceberg::table_properties_t>& props) {
    parquet_write_config config;
    if (!props) {
        return config;
    }
    auto it = props->find(codec_key);
    if (it == props->end()) {
        return config;
    }
    // Iceberg convention: codec values are case-insensitive.
    auto codec = it->second;
    std::ranges::transform(
      codec, codec.begin(), [](unsigned char c) { return std::tolower(c); });
    auto compress = string_switch<std::optional<bool>>(codec)
                      .match("zstd", true)
                      .match("uncompressed", false)
                      .match("none", false)
                      .default_match(std::nullopt);
    if (compress.has_value()) {
        config.compress = *compress;
    } else {
        vlog(
          datalake_log.warn,
          "unsupported write.parquet.compression-codec '{}', falling back to "
          "zstd",
          it->second);
    }
    return config;
}

} // namespace datalake
