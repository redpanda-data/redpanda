#pragma once
#include "cluster/types.h"
#include "controller.h"
#include "rpc/deserialize.h"
#include "rpc/serialize.h"
#include "seastarx.h"
#include "storage/record_batch_builder.h"

namespace cluster {

struct simple_batch_builder final : public storage::record_batch_builder {
    using storage::record_batch_builder::record_batch_builder;

    template<typename K, typename V>
    simple_batch_builder& add_kv(K key, V value) {
        add_raw_kv(
          rpc::serialize(std::move(key)), rpc::serialize(std::move(value)));
        return *this;
    }
};
} // namespace cluster
