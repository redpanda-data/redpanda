/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/controller_snapshot.h"

#include "security/types.h"
#include "serde/rw/rw.h"

namespace cluster {

namespace controller_snapshot_parts {

template<typename Vec>
ss::future<> write_vector_async(iobuf& out, Vec t) {
    if (unlikely(t.size() > std::numeric_limits<serde::serde_size_t>::max())) {
        throw serde::serde_exception(fmt_with_ctx(
          ssx::sformat,
          "serde: vector size {} exceeds serde_size_t",
          t.size()));
    }
    serde::write(out, static_cast<serde::serde_size_t>(t.size()));
    return ss::do_with(std::move(t), [&out](Vec& t) {
        return ss::do_for_each(
          t, [&out](auto& el) { serde::write(out, std::move(el)); });
    });
}

template<typename Map>
ss::future<> write_map_async(iobuf& out, Map t) {
    using serde::write;
    if (unlikely(t.size() > std::numeric_limits<serde::serde_size_t>::max())) {
        throw serde::serde_exception(fmt_with_ctx(
          ssx::sformat, "serde: map size {} exceeds serde_size_t", t.size()));
    }
    write(out, static_cast<serde::serde_size_t>(t.size()));
    return ss::do_with(std::move(t), [&out](Map& t) {
        return ss::do_for_each(t, [&out](auto& el) {
            write(out, el.first);
            return serde::write_async(out, std::move(el.second));
        });
    });
}

template<typename T>
concept Reservable = requires(T t) { t.reserve(0U); };

template<typename Vec>
ss::future<Vec>
read_vector_async_nested(iobuf_parser& in, const std::size_t bytes_left_limit) {
    using value_type = typename Vec::value_type;
    const auto size = serde::read_nested<serde::serde_size_t>(
      in, bytes_left_limit);
    return ss::do_with(Vec{}, [size, &in, bytes_left_limit](Vec& t) {
        if constexpr (Reservable<Vec>) {
            t.reserve(size);
        }
        return ss::do_until(
                 [size, &t] { return t.size() == size; },
                 [&t, &in, bytes_left_limit] {
                     t.push_back(
                       serde::read_nested<value_type>(in, bytes_left_limit));
                     return ss::now();
                 })
          .then([&t] {
              t.shrink_to_fit();
              return std::move(t);
          });
    });
}

template<typename Map>
ss::future<Map>
read_map_async_nested(iobuf_parser& in, const std::size_t bytes_left_limit) {
    using key_type = typename Map::key_type;
    using mapped_type = typename Map::mapped_type;
    const auto size = serde::read_nested<serde::serde_size_t>(
      in, bytes_left_limit);
    return ss::do_with(Map{}, [size, &in, bytes_left_limit](Map& t) {
        if constexpr (Reservable<Map>) {
            t.reserve(size);
        }
        return ss::do_until(
                 [size, &t] { return t.size() == size; },
                 [&t, &in, bytes_left_limit] {
                     return serde::read_async_nested<key_type>(
                              in, bytes_left_limit)
                       .then([&t, &in, bytes_left_limit](auto key) {
                           return serde::read_async_nested<mapped_type>(
                                    in, bytes_left_limit)
                             .then(
                               [&t, key = std::move(key)](auto val) mutable {
                                   t.emplace(std::move(key), std::move(val));
                               });
                       });
                 })
          .then([&t] { return std::move(t); });
    });
}

ss::future<> topics_t::topic_t::serde_async_write(iobuf& out) {
    serde::write(out, metadata);
    co_await write_map_async(out, std::move(partitions));
    co_await write_map_async(out, std::move(updates));
    serde::write(out, disabled_set);
}

ss::future<>
topics_t::topic_t::serde_async_read(iobuf_parser& in, const serde::header h) {
    metadata = serde::read_nested<decltype(metadata)>(in, h._bytes_left_limit);
    partitions = co_await read_map_async_nested<decltype(partitions)>(
      in, h._bytes_left_limit);
    updates = co_await read_map_async_nested<decltype(updates)>(
      in, h._bytes_left_limit);
    if (h._version >= 1) {
        disabled_set = serde::read_nested<decltype(disabled_set)>(
          in, h._bytes_left_limit);
    }

    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

ss::future<> topics_t::serde_async_write(iobuf& out) {
    co_await write_map_async(out, std::move(topics));
    serde::write(out, highest_group_id);
    co_await write_map_async(out, std::move(lifecycle_markers));
    co_await write_map_async(out, partitions_to_force_recover);
}

ss::future<>
topics_t::serde_async_read(iobuf_parser& in, const serde::header h) {
    topics = co_await read_map_async_nested<decltype(topics)>(
      in, h._bytes_left_limit);
    highest_group_id = serde::read_nested<decltype(highest_group_id)>(
      in, h._bytes_left_limit);
    lifecycle_markers
      = co_await read_map_async_nested<decltype(lifecycle_markers)>(
        in, h._bytes_left_limit);

    if (h._version >= 1) {
        partitions_to_force_recover
          = co_await read_map_async_nested<force_recoverable_partitions_t>(
            in, h._bytes_left_limit);
    }

    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

ss::future<> security_t::serde_async_write(iobuf& out) {
    co_await write_vector_async(out, std::move(user_credentials));
    co_await write_vector_async(out, std::move(acls));
    co_await write_vector_async(out, std::move(roles));
}

ss::future<>
security_t::serde_async_read(iobuf_parser& in, const serde::header h) {
    user_credentials
      = co_await read_vector_async_nested<decltype(user_credentials)>(
        in, h._bytes_left_limit);
    acls = co_await read_vector_async_nested<decltype(acls)>(
      in, h._bytes_left_limit);
    if (h._version > 0) {
        roles = co_await read_vector_async_nested<decltype(roles)>(
          in, h._bytes_left_limit);
    }

    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

} // namespace controller_snapshot_parts

ss::future<> controller_snapshot::serde_async_write(iobuf& out) {
    co_await serde::write_async(out, std::move(bootstrap));
    co_await serde::write_async(out, std::move(features));
    co_await serde::write_async(out, std::move(members));
    co_await serde::write_async(out, std::move(config));
    co_await serde::write_async(out, std::move(topics));
    co_await serde::write_async(out, std::move(security));
    co_await serde::write_async(out, std::move(metrics_reporter));
    co_await serde::write_async(out, std::move(plugins));
    co_await serde::write_async(out, std::move(cluster_recovery));
    co_await serde::write_async(out, std::move(client_quotas));
    co_await serde::write_async(out, std::move(data_migrations));
}

ss::future<>
controller_snapshot::serde_async_read(iobuf_parser& in, const serde::header h) {
    bootstrap = co_await serde::read_async_nested<decltype(bootstrap)>(
      in, h._bytes_left_limit);
    features = co_await serde::read_async_nested<decltype(features)>(
      in, h._bytes_left_limit);
    members = co_await serde::read_async_nested<decltype(members)>(
      in, h._bytes_left_limit);
    config = co_await serde::read_async_nested<decltype(config)>(
      in, h._bytes_left_limit);
    topics = co_await serde::read_async_nested<decltype(topics)>(
      in, h._bytes_left_limit);
    security = co_await serde::read_async_nested<decltype(security)>(
      in, h._bytes_left_limit);
    metrics_reporter
      = co_await serde::read_async_nested<decltype(metrics_reporter)>(
        in, h._bytes_left_limit);

    if (h._version >= 1) {
        plugins = co_await serde::read_async_nested<decltype(plugins)>(
          in, h._bytes_left_limit);
    }
    if (h._version >= 2) {
        cluster_recovery
          = co_await serde::read_async_nested<decltype(cluster_recovery)>(
            in, h._bytes_left_limit);
    }
    if (h._version >= 3) {
        client_quotas
          = co_await serde::read_async_nested<decltype(client_quotas)>(
            in, h._bytes_left_limit);
    }

    if (h._version >= 4) {
        data_migrations
          = co_await serde::read_async_nested<decltype(data_migrations)>(
            in, h._bytes_left_limit);
    }

    if (in.bytes_left() > h._bytes_left_limit) {
        in.skip(in.bytes_left() - h._bytes_left_limit);
    }
}

} // namespace cluster
