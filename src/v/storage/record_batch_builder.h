#pragma once
#include "bytes/iobuf.h"
#include "model/record.h"
#include "seastarx.h"
#include "utils/vint.h"

namespace storage {
class record_batch_builder {
public:
    record_batch_builder(model::record_batch_type, model::offset);

    virtual record_batch_builder& add_raw_kv(iobuf&& key, iobuf&& value) {
        _records.emplace_back(std::move(key), std::move(value));
        return *this;
    }
    virtual model::record_batch build() &&;
    virtual ~record_batch_builder();

private:
    static constexpr vint::value_type zero_vint_size = vint::vint_size(0);
    struct serialized_record {
        serialized_record(iobuf k, iobuf v)
          : key(std::move(k))
          , value(std::move(v)) {
        }

        iobuf key;
        iobuf value;

        uint32_t size_bytes() const {
            return key.size_bytes() + value.size_bytes();
        }
    };

    uint32_t record_size(int32_t offset_delta, const serialized_record& r);

    model::record_batch_type _batch_type;
    model::offset _base_offset;
    model::compression _compression;
    std::vector<serialized_record> _records;
};
} // namespace storage
