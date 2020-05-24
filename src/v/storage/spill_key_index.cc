#include "storage/spill_key_index.h"

#include "bytes/bytes.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/compacted_index_writer.h"
#include "storage/logger.h"
#include "utils/vint.h"
#include "vlog.h"

#include <seastar/core/file.hh>
#include <seastar/core/future-util.hh>

#include <fmt/ostream.h>

namespace storage::internal {
using namespace storage; // NOLINT
// use *exactly* 1 write-behind buffer for keys sice it will be an in memory
// workload for most workloads. No need to waste memory here

using vint_array = std::array<uint8_t, vint::max_length>;

static constexpr const std::array<char, 1> key_entry_type{
  (char)compacted_index::entry_type::key};

static constexpr const std::array<char, 1> truncate_entry_type{
  (char)compacted_index::entry_type::truncation};

spill_key_index::spill_key_index(
  ss::sstring name,
  ss::file index_file,
  ss::io_priority_class p,
  size_t max_memory)
  : compacted_index_writer::impl(std::move(name))
  , _appender(std::move(index_file), segment_appender::options(p, 1))
  , _max_mem(max_memory) {}

ss::future<>
spill_key_index::index(bytes_view v, model::offset base_offset, int32_t delta) {
    if (auto it = _midx.find(v); it != _midx.end()) {
        auto& pair = it->second;
        if (base_offset > pair.base_offset) {
            pair.base_offset = base_offset;
            pair.delta = delta;
        }
        return ss::now();
    }
    // not found
    return add_key(bytes(v), value_type{base_offset, delta});
}

ss::future<> spill_key_index::add_key(bytes b, value_type v) {
    return ss::do_until(
             [this, sz = b.size()] {
                 // stop condition
                 return _midx.empty() || _mem_usage + sz < _max_mem;
             },
             [this] {
                 // evict random entry
                 auto mit = _midx.begin();
                 auto n = random_generators::get_int<size_t>(
                   0, _midx.size() - 1);
                 std::advance(mit, n);
                 auto node = _midx.extract(mit);
                 bytes key = node.key();
                 value_type o = node.mapped();
                 _mem_usage -= key.size();
                 vlog(stlog.trace, "evicting key: {}", key);
                 return ss::do_with(
                   std::move(key), o, [this](const bytes& k, value_type o) {
                       return spill(compacted_index::entry_type::key, k, o);
                   });
             })
      .then([this, b = std::move(b), v]() mutable {
          // convert iobuf to key
          _mem_usage += b.size();
          _midx.insert({std::move(b), v});
      });
}
ss::future<> spill_key_index::index(
  const iobuf& key, model::offset base_offset, int32_t delta) {
    if (auto it = _midx.find(key); it != _midx.end()) {
        auto& pair = it->second;
        if (base_offset > pair.base_offset) {
            pair.base_offset = base_offset;
            pair.delta = delta;
        }
        return ss::now();
    }
    // not found
    return add_key(iobuf_to_bytes(key), value_type{base_offset, delta});
}

static inline ss::future<>
write_one_vint(segment_appender& appender, uint8_t* data, size_t size) {
    return appender
      // NOLINTNEXTLINE
      .append(reinterpret_cast<const char*>(data), size);
}

// format is:
// byte   key-type
// vint   size-key
// []byte key
// vint   offset
// vint   delta
ss::future<> spill_key_index::spill(
  compacted_index::entry_type type, bytes_view b, value_type v) {
    ++_footer.keys;
    auto f = ss::now();
    if (likely(type == compacted_index::entry_type::key)) {
        f = _appender.append(key_entry_type.data(), key_entry_type.size());
    } else if (type == compacted_index::entry_type::truncation) {
        f = _appender.append(
          truncate_entry_type.data(), truncate_entry_type.size());
    }

    return f.then([this, b, v] {
        return ss::do_with(vint_array(), [this, b, v](vint_array& ref) {
            const size_t size = vint::serialize(b.size(), ref.data());
            _footer.size += size;
            _crc.extend(ref.data(), ref.size());
            return write_one_vint(_appender, ref.data(), size)
              .then([this, b] {
                  _footer.size += b.size();
                  _crc.extend(b.data(), b.size());
                  return _appender.append(b);
              })
              .then([this, v, &ref] {
                  const size_t size = vint::serialize(
                    v.base_offset, ref.data());
                  _footer.size += size;
                  _crc.extend(ref.data(), ref.size());
                  return write_one_vint(_appender, ref.data(), size);
              })
              .then([this, v, &ref] {
                  const size_t size = vint::serialize(v.delta, ref.data());
                  _footer.size += size;
                  _crc.extend(ref.data(), ref.size());
                  return write_one_vint(_appender, ref.data(), size);
              });
        });
    });
}

ss::future<> spill_key_index::drain_all_keys() {
    return ss::do_until(
      [this] {
          // stop condition
          return _midx.empty();
      },
      [this] {
          auto node = _midx.extract(_midx.begin());
          bytes key = node.key();
          value_type o = node.mapped();
          _mem_usage -= key.size();
          return ss::do_with(
            std::move(key), o, [this](const bytes& k, value_type o) {
                return spill(compacted_index::entry_type::key, k, o);
            });
      });
}

ss::future<> spill_key_index::truncate(model::offset o) {
    _footer.flags |= compacted_index::footer_flags::truncation;
    return drain_all_keys().then([this, o] {
        static constexpr std::string_view compacted_key = "compaction";
        return spill(
          compacted_index::entry_type::truncation,
          bytes_view(
            // NOLINTNEXTLINE
            reinterpret_cast<const uint8_t*>(compacted_key.data()),
            compacted_key.size()),
          // this is actually the base_offset + max_delta so everything upto and
          // including this offset must be ignored during self compaction
          value_type{o, 0});
    });
}

ss::future<> spill_key_index::close() {
    return drain_all_keys().then([this] {
        vassert(
          _mem_usage == 0,
          "Failed to drain all keys, {} bytes left",
          _mem_usage);
        _footer.crc = _crc.value();
        return ss::do_with(
                 reflection::to_iobuf(_footer),
                 [this](iobuf& b) { return _appender.append(b); })
          .then([this] { return _appender.close(); });
    });
}

void spill_key_index::print(std::ostream& o) const { o << *this; }

std::ostream& operator<<(std::ostream& o, const spill_key_index& k) {
    fmt::print(
      o,
      "{{name:{}, max_mem:{}, mem_usage:{}, persisted_entries:{}, "
      "in_memory_entries:{}, file_appender:{}}}",
      k.filename(),
      k._max_mem,
      k._max_mem,
      k._footer.keys,
      k._midx.size(),
      k._appender);
    return o;
}

} // namespace storage::internal

namespace storage {
compacted_index_writer make_file_backed_compacted_index(
  ss::sstring name, ss::file f, ss::io_priority_class p, size_t max_memory) {
    return compacted_index_writer(std::make_unique<internal::spill_key_index>(
      std::move(name), std::move(f), p, max_memory));
}

} // namespace storage
