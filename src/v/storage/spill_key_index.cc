#include "storage/spill_key_index.h"

#include "bytes/bytes.h"
#include "random/generators.h"
#include "storage/compacted_topic_index.h"
#include "storage/logger.h"
#include "vlog.h"

#include <seastar/core/file.hh>
#include <seastar/core/future-util.hh>

#include <fmt/ostream.h>

namespace storage {
spill_key_index::spill_key_index(
  ss::file index_file, ss::io_priority_class p, size_t max_memory)
  : _index(make_file_backed_compacted_index(std::move(index_file), p))
  , _max_mem(max_memory) {}

ss::future<> spill_key_index::index(const iobuf& key, model::offset o) {
    if (auto it = _midx.find(key); it != _midx.end()) {
        it->second = std::max(it->second, o);
        return ss::now();
    }
    // not found
    return ss::do_until(
             [this, &key] {
                 // stop condition
                 return _mem_usage + key.size_bytes() < _max_mem
                        || _midx.empty();
             },
             [this] {
                 // evict random entry
                 auto mit = _midx.begin();
                 auto n = random_generators::get_int<size_t>(
                   0, _midx.size() - 1);
                 std::advance(mit, n);
                 auto node = _midx.extract(mit);
                 _mem_usage -= node.key().size();
                 vlog(stlog.trace, "evicting key: {}", node.key());
                 return ss::do_with(
                   std::move(node.key()),
                   node.mapped(),
                   [this](const bytes& k, model::offset o) {
                       return _index.write_key(k, o);
                   });
             })
      .then([this, &key, o] {
          // convert iobuf to key
          _mem_usage += key.size_bytes();
          _midx.emplace(iobuf_to_bytes(key), o);
      });
}

ss::future<> spill_key_index::close() { return _index.close(); }

} // namespace storage
