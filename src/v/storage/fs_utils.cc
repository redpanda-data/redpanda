/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "fs_utils.h"

#include "config/node_config.h"
#include "model/namespace.h"

namespace storage {

ss::sstring segment_full_path::string() const {
    if (unlikely(override_path.has_value())) {
        return *override_path;
    }

    return ss::format(
      "{}/{}_{}/{}-{}-{}{}",
      dir_part.base_dir.value_or(config::node().data_directory().as_sstring()),
      dir_part.ntp.path(),
      dir_part.revision_id,
      file_part.base_offset(),
      file_part.term(),
      to_string(file_part.version),
      extension);
}

ss::sstring partition_path::make_string() const {
    return ss::format(
      "{}/{}_{}",
      base_dir.value_or(config::node().data_directory().as_sstring()),
      ntp.path(),
      revision_id);
}

partition_path::partition_path(const ntp_config& ntpc)
  : ntp(ntpc.ntp())
  , revision_id(ntpc.get_revision()) {
    /**
     * We generally avoid storing the base path on every instance:
     * only use it as an override for unit tests.
     */
    if (
      // Unit tests that set a random directory in the config
      ntpc.base_directory() != config::node().data_directory().as_sstring()
      // Unit tests that set a random (relative) config in node_config
      || (ntpc.base_directory().size() && ntpc.base_directory()[0] != '/')) {
        base_dir = ntpc.base_directory();
    }
}

std::optional<segment_full_path> segment_full_path::parse(
  partition_path const& dir_part, const ss::sstring& filename) noexcept {
    std::optional<segment_path::metadata> file_part_opt;
    try {
        file_part_opt = segment_path::parse_segment_filename(filename);
    } catch (...) {
        return std::nullopt;
    }
    if (!file_part_opt) {
        return std::nullopt;
    }

    return segment_full_path(
      partition_path(dir_part), std::move(*file_part_opt));
}

segment_full_path segment_full_path::mock(ss::sstring str_path) {
    auto ntp = model::ntp(
      model::kafka_namespace,
      model::topic_partition(
        model::topic{"testtopic"}, model::partition_id{0}));

    auto pp = partition_path(ntp, model::revision_id{123});

    auto sp = segment_path::metadata{
      .base_offset = model::offset{123},
      .term = model::term_id{456},
      .version = record_version_type::v1};

    return segment_full_path(str_path, pp, sp);
}

bool segment_full_path::is_internal_topic() const {
    return dir_part.ntp.ns != model::kafka_namespace;
}

segment_full_path segment_full_path::to_index() const {
    if (extension == ".log") {
        return with_extension(".base_index");
    } else if (extension == ".log.compaction.staging") {
        return with_extension(".log.compaction.base_index");
    } else {
        vassert(false, "Unexpected extension {}", extension);
    }
}

segment_full_path segment_full_path::to_compacted_index() const {
    if (extension == ".log") {
        return with_extension(".compaction_index");
    } else if (extension == ".log.compaction.staging") {
        return with_extension("log.compaction.compaction_index");
    } else {
        vassert(false, "Unexpected extension {}", extension);
    }
}

segment_full_path segment_full_path::to_compaction_staging() const {
    vassert(extension == ".log", "Unexpected extension {}", extension);
    return with_extension(".log.compaction.staging");
}

segment_full_path segment_full_path::to_staging() const {
    if (extension == ".log") {
        return with_extension(".log.staging");
    } else if (extension == ".log.compaction.staging") {
        return with_extension(".log.compaction.staging.staging");
    } else {
        vassert(false, "Unexpected extension {}", extension);
    }
}

std::ostream& operator<<(std::ostream& o, const partition_path& p) {
    o << ss::format("{}_{}", p.ntp.path(), p.revision_id);
    return o;
}

std::ostream& operator<<(std::ostream& o, const segment_full_path& p) {
    o << p.string();
    return o;
}

} // namespace storage