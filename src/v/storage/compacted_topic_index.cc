#include "storage/compacted_topic_index.h"

#include "storage/segment_appender.h"
#include "utils/vint.h"

#include <seastar/core/file.hh>

#include <absl/container/btree_map.h>

namespace storage {
class file_compacted_index final : public compacted_topic_index::impl {
public:
    // use *exactly* 1 write-behind buffer for keys sice it will be an in memory
    // workload for most workloads. No need to waste memory here
    file_compacted_index(ss::file f, ss::io_priority_class prio) noexcept
      : _appender(std::move(f), segment_appender::options(prio, 1)) {}

    file_compacted_index(const file_compacted_index&) = delete;
    file_compacted_index& operator=(const file_compacted_index&) = delete;
    file_compacted_index(file_compacted_index&&) noexcept = default;
    file_compacted_index& operator=(file_compacted_index&&) noexcept = delete;

    ~file_compacted_index() noexcept final = default;

    // format is:
    // vint   size-key
    // []byte key
    // vint   offset
    ss::future<> write_key(const bytes& b, model::offset o) final {
        return ss::do_with(
          std::array<int8_t, vint::max_length>(),
          [this, &b, o](std::array<int8_t, vint::max_length>& ref) {
              const size_t size = vint::serialize(b.size(), ref.data());
              return _appender
                // NOLINTNEXTLINE
                .append(reinterpret_cast<const char*>(ref.data()), size)
                .then([this, &b] { return _appender.append(b); })
                .then([this, o, &ref] {
                    const size_t size = vint::serialize(o, ref.data());
                    return _appender
                      // NOLINTNEXTLINE
                      .append(reinterpret_cast<const char*>(ref.data()), size);
                });
          });
    }
    ss::future<> close() final { return _appender.close(); }

private:
    segment_appender _appender;
};

compacted_topic_index
make_file_backed_compacted_index(ss::file f, ss::io_priority_class p) {
    return compacted_topic_index(
      std::make_unique<file_compacted_index>(std::move(f), p));
}

} // namespace storage
