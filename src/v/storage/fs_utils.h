/*
 * Copyright 2020 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#pragma once
#include "model/fundamental.h"
#include "seastarx.h"
#include "storage/ntp_config.h"
#include "storage/version.h"

#include <seastar/core/print.hh>
#include <seastar/core/sstring.hh>

#include <boost/lexical_cast.hpp>

#include <filesystem>
#include <regex>

namespace storage {

struct segment_path {
    struct metadata {
        model::offset base_offset;
        model::term_id term;
        record_version_type version;
    };

    /// Construct an absolute path to a segment file.
    static std::filesystem::path make_segment_path(
      const ntp_config& ntp_config,
      const model::offset& base_offset,
      const model::term_id& term,
      const record_version_type& version) {
        return std::filesystem::path(ss::format(
          "{}/{}-{}-{}.log",
          ntp_config.work_directory(),
          base_offset(),
          term(),
          to_string(version)));
    }

    /// Parse metadata from a segment filename.
    static std::optional<metadata>
    parse_segment_filename(const ss::sstring& name) {
        const std::regex re(R"(^(\d+)-(\d+)-([\x00-\x7F]+).log$)");
        std::cmatch match;
        if (!std::regex_match(name.c_str(), match, re)) {
            return std::nullopt;
        }
        return metadata{
          .base_offset = model::offset(
            boost::lexical_cast<uint64_t>(match[1].str())),
          .term = model::term_id(boost::lexical_cast<int64_t>(match[2].str())),
          .version = from_string(match[3].str()),
        };
    }
};

class segment_full_path;

class partition_path {
public:
    struct metadata {
        model::partition_id partition_id;
        model::revision_id revision_id;
    };

    /// Parse ntp directory name
    static std::optional<metadata>
    parse_partition_directory(const ss::sstring& name) {
        const std::regex re(R"(^(\d+)_(\d+)$)");
        std::cmatch match;
        if (!std::regex_match(name.c_str(), match, re)) {
            return std::nullopt;
        }
        return metadata{
          .partition_id = model::partition_id(
            boost::lexical_cast<uint64_t>(match[1].str())),
          .revision_id = model::revision_id(
            boost::lexical_cast<uint64_t>(match[2].str()))};
    }

    partition_path(const ntp_config& ntpc);

    operator std::filesystem::path() const { return make_path(); }
    operator ss::sstring() const { return make_string(); }

private:
    // If base_dir is null, use the global node_config::data_directory: this
    // enables overriding the dir, while avoiding copying it everywhere in
    // normal operation.
    std::optional<ss::sstring> base_dir;

    model::ntp ntp;
    model::revision_id revision_id;

    // For segment_full_path::mock() in unit tests
    partition_path(model::ntp ntp, model::revision_id r)
      : ntp(ntp)
      , revision_id(r) {}

    std::filesystem::path make_path() const {
        return std::filesystem::path(make_string());
    }
    ss::sstring make_string() const;
    friend std::ostream& operator<<(std::ostream&, const partition_path&);
    friend class segment_full_path;
};

class segment_full_path {
public:
    segment_full_path(
      const ntp_config& ntpc,
      model::offset o,
      model::term_id t,
      record_version_type v)
      : dir_part(ntpc)
      , file_part({.base_offset = o, .term = t, .version = v}) {}

    segment_full_path(
      partition_path&& dir_part, segment_path::metadata&& file_part)
      : dir_part(std::move(dir_part))
      , file_part(std::move(file_part)) {}

    /**
     * Transformations from regular segment paths to the index etc
     * for that segment.  These are strict: if you try to e.g. make
     * an index path from an index path, that will assert out.
     */
    segment_full_path to_index() const;
    segment_full_path to_compacted_index() const;
    segment_full_path to_compaction_staging() const;
    segment_full_path to_staging() const;

    /**
     * Hydrate the metadata into a fully qualified filesystem path.
     */
    ss::sstring string() const;
    operator ss::sstring() const { return string(); }
    operator std::filesystem::path() const {
        return std::filesystem::path(string());
    }

    /**
     * For unit tests: construct a path object that has a raw filename inside
     * it, and some metadata that doesn't have to match that filename.
     */
    static segment_full_path mock(ss::sstring str_path);

    /**
     * On malformed input, returns nullopt (does not throw)
     */
    static std::optional<segment_full_path>
    parse(const partition_path& dir_part, const ss::sstring& filename) noexcept;

    model::term_id get_term() { return file_part.term; }
    model::offset get_base_offset() { return file_part.base_offset; }
    record_version_type get_version() { return file_part.version; };
    const model::ntp& get_ntp() const { return dir_part.ntp; }

    /**
     * This is a storage-specific interpretation of 'internal': it means
     * topics that are not expected to container end user data, and therefore
     * to have all batches considered for indexing, whereas for user data we
     * only want to index their raft_data batches (because including other
     * batches disrupts time indexing when user timestamps diverge from ours).
     */
    bool is_internal_topic() const;

private:
    // This should always point to a compile-time string constant: we do not
    // use any dynamic extensions, just simple thing like ".index" and
    // ".log.staging"
    std::string_view extension{".log"};

    /***
     * Return a new instance with the extension set to `ext`, replacing
     * any extension on the current instance.
     *
     * `ext` should include a '.' character
     *
     * This should always point to a compile-time string constant: we do not
     * use any dynamic extensions, just simple thing like ".index" and
     * ".log.staging"
     */
    segment_full_path with_extension(std::string_view ext) const {
        auto copy = *this;
        copy.extension = ext;
        return copy;
    }

    /// For use in unit tests: override the effective path
    std::optional<ss::sstring> override_path{std::nullopt};

    /// State that gives us the directory (of the partition)
    partition_path dir_part;

    /// State that gives us the filename (of the segment, within the partition)
    segment_path::metadata file_part;

    // Private, used by mock() for tests
    segment_full_path(
      ss::sstring override_path,
      partition_path dir_part,
      segment_path::metadata file_part)
      : override_path(override_path)
      , dir_part(dir_part)
      , file_part(file_part) {}

    friend std::ostream& operator<<(std::ostream&, const segment_full_path&);
};

std::ostream& operator<<(std::ostream&, const segment_full_path&);

} // namespace storage
