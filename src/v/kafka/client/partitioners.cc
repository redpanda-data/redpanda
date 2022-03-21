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

#include <functional>
#include <optional>
#include <tuple>
#include <type_traits>

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

} // namespace kafka::client
