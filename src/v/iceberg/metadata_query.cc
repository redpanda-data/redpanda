// Copyright 2024 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "iceberg/metadata_query.h"

namespace iceberg {

namespace {
template<result_type T>
struct results_container {};

template<>
struct results_container<result_type::snapshot> {
    using type = chunked_vector<snapshot>;
};

template<>
struct results_container<result_type::manifest_file> {
    using type = chunked_vector<manifest_file>;
};

template<>
struct results_container<result_type::manifest> {
    using type = chunked_vector<manifest>;
};

template<result_type ResultT>
struct results_collector {
    using result_t = results_container<ResultT>::type;
    void collect(const snapshot& s) {
        if constexpr (ResultT == result_type::snapshot) {
            result_.push_back(s);
        }
    }
    ss::stop_iteration
    collect(const snapshot& snap, const manifest_file& m_file) {
        if constexpr (ResultT == result_type::snapshot) {
            result_.push_back(snap);
            return seastar::stop_iteration::yes;
        } else if constexpr (ResultT == result_type::manifest_file) {
            if (!manifests_.contains(m_file.manifest_path)) {
                manifests_.emplace(m_file.manifest_path);
                result_.push_back(m_file.copy());
            }
            return seastar::stop_iteration::no;
        }
        vassert(
          false,
          "Trying to collect snapshot or manifest_file while result type is: "
          "{}",
          fmt::underlying(ResultT));
    }
    ss::stop_iteration collect(
      const snapshot& snap,
      const manifest_file& m_file,
      const manifest& manifest) {
        if constexpr (ResultT == result_type::snapshot) {
            result_.push_back(snap);
            return seastar::stop_iteration::yes;
        } else if constexpr (ResultT == result_type::manifest_file) {
            if (!manifests_.contains(m_file.manifest_path)) {
                manifests_.emplace(m_file.manifest_path);
                result_.push_back(m_file.copy());
            }
            return seastar::stop_iteration::yes;
        } else if constexpr (ResultT == result_type::manifest) {
            if (!manifests_.contains(m_file.manifest_path)) {
                manifests_.emplace(m_file.manifest_path);
                result_.push_back(manifest.copy());
            }

            return seastar::stop_iteration::no;
        }
        vassert(
          false,
          "Trying to collect snapshot, manifest_file or manifest while result "
          "type is: {}",
          fmt::underlying(ResultT));
    }

    results_container<ResultT>::type release() && {
        manifests_.clear();
        return std::move(result_);
    }

private:
    // container used for deduplication based on path
    chunked_hash_set<ss::sstring> manifests_;
    results_container<ResultT>::type result_;
};

template<result_type ResultT>
bool needs_manifest_list(const metadata_query<ResultT>& query) {
    // manifest list download is required if query needs information from it
    // i.e. it is either querying manifest list or manifest content or is
    // required to collect manifest_list or manifests as a result.
    return query.manifest_file_matcher.has_value()
           || query.manifest_matcher.has_value()
           || query.r_type == result_type::manifest_file
           || query.r_type == result_type::manifest;
}

template<result_type ResultT>
bool needs_manifest(const metadata_query<ResultT>& query) {
    return query.manifest_matcher.has_value()
           || query.r_type == result_type::manifest;
}

template<result_type ResultT>
bool matches(const metadata_query<ResultT>& query, const snapshot& s) {
    return !query.snapshot_matcher.has_value() || (*query.snapshot_matcher)(s);
}
template<result_type ResultT>
bool matches(const metadata_query<ResultT>& query, const manifest_file& s) {
    return !query.manifest_file_matcher.has_value()
           || (*query.manifest_file_matcher)(s);
}
template<result_type ResultT>
bool matches(const metadata_query<ResultT>& query, const manifest& s) {
    return !query.manifest_matcher.has_value() || (*query.manifest_matcher)(s);
}

checked<
  std::reference_wrapper<const schema>,
  iceberg::metadata_query_executor::errc>
get_snapshot_schema(const table_metadata& table, const snapshot& snap) {
    auto s_it = std::ranges::find(
      table.schemas, snap.schema_id, &schema::schema_id);
    if (s_it == table.schemas.end()) {
        return iceberg::metadata_query_executor::errc::
          table_metadata_inconsistency;
    }
    return std::ref(*s_it);
}

checked<partition_key_type, iceberg::metadata_query_executor::errc>
make_partition_key_type(
  const table_metadata& table,
  const schema& schema,
  const manifest_file& m_file) {
    auto p_spec_it = std::ranges::find(
      table.partition_specs,
      m_file.partition_spec_id,
      &partition_spec::spec_id);

    if (p_spec_it == table.partition_specs.end()) {
        return iceberg::metadata_query_executor::errc::
          table_metadata_inconsistency;
    }

    return partition_key_type::create(*p_spec_it, schema);
}

} // namespace

template<result_type ResultT>
ss::future<checked<
  typename results_container<ResultT>::type,
  metadata_query_executor::errc>>
do_execute_query(
  const table_metadata& table,
  manifest_io& io,
  const metadata_query<ResultT>& query) {
    results_collector<ResultT> collector;

    if (!table.snapshots) {
        co_return std::move(collector).release();
    }
    const auto& snapshots = table.snapshots.value();
    // simple DFS scan over metadata
    for (const auto& s : snapshots) {
        if (!matches(query, s)) {
            continue;
        }
        // snapshot matches, check if manifest list is needed, otherwise simply
        // collect the snapshot
        if (!needs_manifest_list(query)) {
            // this is a special case as if manifest list is not required we
            // should keep checking another segment.
            collector.collect(s);
            continue;
        }
        auto m_list_result = co_await io.download_manifest_list_uri(
          s.manifest_list_path);

        if (m_list_result.has_error()) {
            vlog(
              log.warn,
              "error downloading manifest list from {} - {}",
              s.manifest_list_path,
              fmt::underlying_t<metadata_io::errc>(m_list_result.error()));
            co_return metadata_query_executor::errc::metadata_io_error;
        }
        auto schema_result = get_snapshot_schema(table, s);
        if (schema_result.has_error()) {
            co_return schema_result.error();
        }
        const auto& schema = schema_result.value().get();
        for (auto& manifest_file : m_list_result.value().files) {
            if (!matches(query, manifest_file)) {
                continue;
            }
            if (!needs_manifest(query)) {
                if (collector.collect(s, manifest_file)) {
                    break;
                }
                continue;
            }
            auto pk_result = make_partition_key_type(
              table, schema, manifest_file);
            if (pk_result.has_error()) {
                co_return pk_result.error();
            }
            auto m_result = co_await io.download_manifest_uri(
              manifest_file.manifest_path, std::move(pk_result.value()));

            if (m_result.has_error()) {
                vlog(
                  log.warn,
                  "error downloading manifest from {} - {}",
                  manifest_file.manifest_path,
                  fmt::underlying_t<metadata_io::errc>(m_result.error()));
                co_return metadata_query_executor::errc::metadata_io_error;
            }

            if (matches(query, m_result.value())) {
                if (collector.collect(s, manifest_file, m_result.value())) {
                    break;
                }
            }
        }
    }

    co_return std::move(collector).release();
}

ss::future<checked<chunked_vector<snapshot>, metadata_query_executor::errc>>
metadata_query_executor::execute_query(
  const metadata_query<result_type::snapshot>& query) const {
    return do_execute_query(*table_, *io_, query);
}

ss::future<
  checked<chunked_vector<manifest_file>, metadata_query_executor::errc>>
metadata_query_executor::execute_query(
  const metadata_query<result_type::manifest_file>& query) const {
    return do_execute_query(*table_, *io_, query);
}

ss::future<checked<chunked_vector<manifest>, metadata_query_executor::errc>>
metadata_query_executor::execute_query(
  const metadata_query<result_type::manifest>& query) const {
    return do_execute_query(*table_, *io_, query);
}
} // namespace iceberg
