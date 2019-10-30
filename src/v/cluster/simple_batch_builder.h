#pragma once
#include "cluster/types.h"
#include "controller.h"
#include "hashing/crc32c.h"
#include "model/record.h"
#include "model/timeout_clock.h"
#include "rpc/deserialize.h"
#include "rpc/serialize.h"
#include "seastarx.h"
#include "storage/constants.h"
#include "storage/crc_record.h"
#include "utils/fragbuf.h"
#include "utils/memory_data_source.h"
#include "utils/vint.h"

namespace cluster {
/// This is an utility to easily build model::record_batch'es

class simple_batch_builder {
public:
    simple_batch_builder(model::record_batch_type);

    template<typename K, typename V>
    CONCEPT(requires rpc::RPCSerializable<K>&& rpc::RPCSerializable<V>)
    simple_batch_builder& add_kv(K key, V value) {
        return add_raw_kv(
          rpc::serialize(std::move(key)), rpc::serialize(std::move(value)));
    }

    simple_batch_builder& add_raw_kv(fragbuf&& key, fragbuf&& value) {
        _records.emplace_back(std::move(key), std::move(value));
        return *this;
    }

    model::record_batch build() &&;

private:
    static constexpr auto zero_vint_size = vint::vint_size(0);
    struct serialized_record {
        serialized_record(fragbuf k, fragbuf v)
          : key(std::move(k))
          , value(std::move(v)) {
        }

        fragbuf key;
        fragbuf value;
    };

    uint32_t record_size(int32_t offset_delta, const serialized_record& r);

    std::vector<serialized_record> _records;
    model::compression _compression;
    model::record_batch_type _batch_type;
};
} // namespace cluster