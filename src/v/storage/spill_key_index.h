#pragma once
#include "bytes/bytes.h"
#include "hashing/crc32c.h"
#include "model/fundamental.h"
#include "storage/compacted_index.h"
#include "storage/compacted_index_writer.h"
#include "storage/segment_appender.h"

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>

#include <absl/container/flat_hash_map.h>
#include <absl/hash/hash.h>

namespace storage::internal {
using namespace storage; // NOLINT
class spill_key_index final : public compacted_index_writer::impl {
public:
    struct value_type {
        model::offset base_offset;
        int32_t delta{0};
    };
    using underlying_t
      = absl::flat_hash_map<bytes, value_type, bytes_type_hash, bytes_type_eq>;

    spill_key_index(
      ss::sstring filename,
      ss::file index_file,
      ss::io_priority_class,
      size_t max_memory);
    spill_key_index(const spill_key_index&) = delete;
    spill_key_index& operator=(const spill_key_index&) = delete;
    spill_key_index(spill_key_index&&) noexcept = default;
    spill_key_index& operator=(spill_key_index&&) noexcept = delete;
    ~spill_key_index() override;

    // public

    ss::future<> index(const iobuf& key, model::offset, int32_t) final;
    ss::future<> index(bytes_view, model::offset, int32_t) final;
    ss::future<> index(bytes&&, model::offset, int32_t) final;
    ss::future<> truncate(model::offset) final;
    ss::future<> close() final;
    void print(std::ostream&) const final;
    void set_flag(compacted_index::footer_flags) final;

private:
    ss::future<> drain_all_keys();
    ss::future<> add_key(bytes b, value_type);
    ss::future<> spill(compacted_index::entry_type, bytes_view, value_type);

    segment_appender _appender;
    underlying_t _midx;
    size_t _max_mem;
    size_t _mem_usage{0};
    compacted_index::footer _footer;
    crc32 _crc;

    friend std::ostream& operator<<(std::ostream&, const spill_key_index&);
};

} // namespace storage::internal
