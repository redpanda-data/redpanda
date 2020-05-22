#include "storage/spill_key_index.h"

#include "bytes/bytes.h"
#include "random/generators.h"
#include "reflection/adl.h"
#include "storage/compacted_topic_index.h"
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

spill_key_index::spill_key_index(
  ss::file index_file, ss::io_priority_class p, size_t max_memory)
  : _appender(std::move(index_file), segment_appender::options(p, 1))
  , _max_mem(max_memory) {}

ss::future<> spill_key_index::index(bytes_view v, model::offset o) {
    if (auto it = _midx.find(v); it != _midx.end()) {
        it->second = std::max(it->second, o);
        return ss::now();
    }
    // not found
    return add_key(bytes(v), o);
}

ss::future<> spill_key_index::add_key(bytes b, model::offset o) {
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
                 model::offset o = node.mapped();
                 _mem_usage -= key.size();
                 vlog(stlog.trace, "evicting key: {}", key);
                 return ss::do_with(
                   std::move(key), o, [this](const bytes& k, model::offset o) {
                       return spill(k, o);
                   });
             })
      .then([this, b = std::move(b), o]() mutable {
          // convert iobuf to key
          _mem_usage += b.size();
          _midx.insert({std::move(b), o});
      });
}
ss::future<> spill_key_index::index(const iobuf& key, model::offset o) {
    if (auto it = _midx.find(key); it != _midx.end()) {
        it->second = std::max(it->second, o);
        return ss::now();
    }
    // not found
    return add_key(iobuf_to_bytes(key), o);
}

// format is:
// vint   size-key
// []byte key
// vint   offset
ss::future<> spill_key_index::spill(bytes_view b, model::offset o) {
    ++_footer.keys;
    return ss::do_with(
      std::array<uint8_t, vint::max_length>(),
      [this, b, o](std::array<uint8_t, vint::max_length>& ref) {
          const size_t size = vint::serialize(b.size(), ref.data());
          _footer.size += size;
          _crc.extend(ref.data(), ref.size());
          return _appender
            // NOLINTNEXTLINE
            .append(reinterpret_cast<const char*>(ref.data()), size)
            .then([this, b] {
                _footer.size += b.size();
                _crc.extend(b.data(), b.size());
                return _appender.append(b);
            })
            .then([this, o, &ref] {
                const size_t size = vint::serialize(o, ref.data());
                _footer.size += size;
                _crc.extend(ref.data(), ref.size());
                return _appender
                  // NOLINTNEXTLINE
                  .append(reinterpret_cast<const char*>(ref.data()), size);
            });
      });
}

ss::future<> spill_key_index::close() {
    return ss::do_until(
             [this] {
                 // stop condition
                 return _midx.empty();
             },
             [this] {
                 auto node = _midx.extract(_midx.begin());
                 bytes key = node.key();
                 model::offset o = node.mapped();
                 _mem_usage -= key.size();
                 return ss::do_with(
                   std::move(key), o, [this](const bytes& k, model::offset o) {
                       return spill(k, o);
                   });
             })
      .then([this] {
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

} // namespace storage::internal

namespace storage {
compacted_topic_index make_file_backed_compacted_index(
  ss::file f, ss::io_priority_class p, size_t max_memory) {
    return compacted_topic_index(
      std::make_unique<internal::spill_key_index>(std::move(f), p, max_memory));
}

} // namespace storage
