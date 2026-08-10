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

#include <absl/container/flat_hash_map.h>

#include <algorithm>
#include <cctype>
#include <charconv>

namespace datalake {

namespace {

constexpr std::string_view codec_key = "write.parquet.compression-codec";
constexpr std::string_view bf_enabled_prefix
  = "write.parquet.bloom-filter-enabled.column.";
constexpr std::string_view bf_ndv_prefix
  = "write.parquet.bloom-filter-ndv.column.";

void parse_compression_codec(
  parquet_write_config& config, const iceberg::table_properties_t& props) {
    auto it = props.find(codec_key);
    if (it == props.end()) {
        return;
    }
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
}

std::optional<bool> parse_bool_value(std::string_view value) {
    ss::sstring lower(value);
    std::ranges::transform(
      lower, lower.begin(), [](unsigned char c) { return std::tolower(c); });
    return string_switch<std::optional<bool>>(lower)
      .match("true", true)
      .match("false", false)
      .default_match(std::nullopt);
}

std::optional<size_t> parse_size_value(std::string_view value) {
    size_t result = 0;
    auto [ptr, ec] = std::from_chars(value.begin(), value.end(), result);
    if (ec != std::errc{} || ptr != value.end()) {
        return std::nullopt;
    }
    return result;
}

void parse_bloom_filter_columns(
  parquet_write_config& config, const iceberg::table_properties_t& props) {
    // Two-pass: first collect enabled flags, then apply NDV overrides.
    // Properties iterate in arbitrary order, so we can't assume enabled
    // comes before ndv.
    struct column_state {
        bool enabled = false;
        std::optional<size_t> ndv;
    };
    absl::flat_hash_map<ss::sstring, column_state> columns;

    for (const auto& [key, value] : props) {
        if (key.starts_with(bf_enabled_prefix)) {
            auto col_name = key.substr(bf_enabled_prefix.size());
            auto parsed = parse_bool_value(value);
            if (!parsed) {
                vlog(
                  datalake_log.warn,
                  "malformed bloom-filter-enabled value '{}' for column '{}', "
                  "ignoring",
                  value,
                  col_name);
                continue;
            }
            columns[col_name].enabled = *parsed;
        } else if (key.starts_with(bf_ndv_prefix)) {
            auto col_name = key.substr(bf_ndv_prefix.size());
            auto parsed = parse_size_value(value);
            if (!parsed) {
                vlog(
                  datalake_log.warn,
                  "malformed bloom-filter-ndv value '{}' for column '{}', "
                  "ignoring",
                  value,
                  col_name);
                continue;
            }
            if (*parsed == 0) {
                vlog(
                  datalake_log.warn,
                  "bloom-filter-ndv of 0 for column '{}' disables the filter, "
                  "ignoring",
                  col_name);
                continue;
            }
            if (*parsed > parquet_write_config::max_bloom_filter_ndv) {
                vlog(
                  datalake_log.warn,
                  "bloom-filter-ndv {} for column '{}' exceeds maximum {}, "
                  "clamping",
                  *parsed,
                  col_name,
                  parquet_write_config::max_bloom_filter_ndv);
                *parsed = parquet_write_config::max_bloom_filter_ndv;
            }
            columns[col_name].ndv = *parsed;
        }
    }

    for (auto& [name, state] : columns) {
        if (!state.enabled) {
            continue;
        }
        config.bloom_filter_columns.push_back({
          .name = name,
          .ndv = state.ndv.value_or(
            parquet_write_config::default_bloom_filter_ndv),
        });
    }
}

} // namespace

parquet_write_config parquet_write_config::from_properties(
  const std::optional<iceberg::table_properties_t>& props) {
    parquet_write_config config;
    if (!props) {
        return config;
    }
    parse_compression_codec(config, *props);
    parse_bloom_filter_columns(config, *props);
    return config;
}

} // namespace datalake
