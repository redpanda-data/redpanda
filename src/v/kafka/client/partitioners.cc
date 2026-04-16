/*
 * Copyright 2021 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "kafka/client/partitioners.h"

#include "hashing/murmur.h"
#include "kafka/client/exceptions.h"
#include "kafka/client/utils.h"
#include "kafka/protocol/errors.h"
#include "kafka/protocol/metadata.h"
#include "random/generators.h"

#include <optional>
#include <tuple>

namespace kafka::client {

namespace detail {

class identity_partitioner final : public partitioner_impl {
public:
    std::optional<model::partition_id>
    operator()(const record_essence& rec, size_t) override {
        return rec.partition_id;
    }
};

class murmur2_key_partitioner final : public partitioner_impl {
public:
    std::optional<model::partition_id>
    operator()(const record_essence& rec, size_t partition_count) override {
        if (!rec.key || rec.key->empty()) {
            return std::nullopt;
        }
        iobuf_const_parser p(*rec.key);
        auto key = p.read_bytes(p.bytes_left());
        auto hash = murmur2(key.data(), key.size());
        return model::partition_id(hash % partition_count);
    }
};

class roundrobin_partitioner final : public partitioner_impl {
public:
    explicit roundrobin_partitioner(model::partition_id initial)
      : partitioner_impl{}
      , _next(initial) {}

    std::optional<model::partition_id>
    operator()(const record_essence&, size_t partition_count) override {
        return model::partition_id(_next++ % partition_count);
    }

private:
    model::partition_id _next;
};

// Try each partitioner in the list until one succeeds.
template<typename... Impls>
class composed_partitioner final : public partitioner_impl {
public:
    explicit composed_partitioner(Impls&&... impls)
      : _impls{std::forward<Impls>(impls)...} {}

    std::optional<model::partition_id>
    operator()(const record_essence& rec, size_t partition_count) override {
        return std::apply(
          [&rec, partition_count, p_id{std::optional<model::partition_id>{}}](
            auto&&... partitioner) mutable {
              return (
                (p_id = p_id.has_value() ? p_id
                                         : partitioner(rec, partition_count)),
                ...);
          },
          _impls);
    }

private:
    std::tuple<Impls...> _impls;
};

} // namespace detail

partitioner identity_partitioner() {
    return partitioner{std::make_unique<detail::identity_partitioner>()};
}

partitioner murmur2_key_partitioner() {
    return partitioner{std::make_unique<detail::murmur2_key_partitioner>()};
}

partitioner roundrobin_partitioner(model::partition_id initial) {
    return partitioner{
      std::make_unique<detail::roundrobin_partitioner>(initial)};
}

partitioner default_partitioner(model::partition_id initial) {
    return partitioner{std::make_unique<detail::composed_partitioner<
      detail::identity_partitioner,
      detail::murmur2_key_partitioner,
      detail::roundrobin_partitioner>>(
      detail::identity_partitioner{},
      detail::murmur2_key_partitioner{},
      detail::roundrobin_partitioner{initial})};
}

void partitioners_cache::apply_metadata(const metadata_update& data) {
    if (!data.topics.has_value()) {
        // No topics in metadata update, nothing to do
        return;
    }
    chunked_hash_set<model::topic> metadata_topics;
    for (const auto& t : *data.topics) {
        static_assert(
          api_version_for(metadata_request::api_type::key) < api_version(12),
          "topic::name is nullable in v12+");
        const auto& t_name = *t.name;
        metadata_topics.emplace(t_name);
        auto it = _partitioners.find(t_name);
        if (
          it != _partitioners.end()
          && it->second.partition_count == t.partitions.size()) {
            // If the topic already exists with the same partition count,
            // we can skip it.
            continue;
        }

        const auto initial_partition_id = model::partition_id{
          random_generators::get_int<model::partition_id::type>(
            t.partitions.size())};

        _partitioners[t_name] = entry{
          .partition_count = t.partitions.size(),
          .partitioner = default_partitioner(initial_partition_id)};
    }
    // remove partitioners for topics that are no longer in the metadata
    // response
    std::erase_if(_partitioners, [&metadata_topics](const auto& entry) {
        return !metadata_topics.contains(entry.first);
    });
}

model::partition_id partitioners_cache::partition_for(
  model::topic_view tv, const record_essence& rec) {
    if (
      auto topic_it = _partitioners.find(tv); topic_it != _partitioners.end()) {
        auto& entry = topic_it->second;
        auto partition_opt = entry.partitioner(rec, entry.partition_count);
        if (partition_opt) {
            return *partition_opt;
        }
    }
    throw topic_error(tv, error_code::unknown_topic_or_partition);
}

} // namespace kafka::client
