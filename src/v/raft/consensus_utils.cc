#include "raft/consensus_utils.h"

#include "likely.h"
#include "model/record.h"
#include "raft/logger.h"
#include "random/generators.h"
#include "resource_mgmt/io_priority.h"
#include "storage/record_batch_builder.h"
#include "utils/state_crc_file.h"
#include "vassert.h"

#include <seastar/core/abort_source.hh>
#include <seastar/core/file-types.hh>
#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>

#include <cstring>
#include <filesystem>
// delete
#include <seastar/core/future-util.hh>

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <iterator>

namespace raft::details {
[[gnu::cold]] void throw_out_of_range() {
    throw std::out_of_range("consensus_utils copy out of bounds");
}
static inline void check_copy_out_of_range(size_t expected, size_t got) {
    if (unlikely(expected != got)) {
        throw_out_of_range();
    }
}

static inline ss::circular_buffer<model::record_batch>
share_n_record_batch(model::record_batch batch, const size_t copies) {
    using ret_t = ss::circular_buffer<model::record_batch>;
    ret_t ret;
    ret.reserve(copies);
    // the fast path
    std::generate_n(
      std::back_inserter(ret), copies, [batch = std::move(batch)]() mutable {
          return batch.share();
      });
    return ret;
}

static inline ss::future<std::vector<ss::circular_buffer<model::record_batch>>>
share_n_batches(
  ss::circular_buffer<model::record_batch> batches, const size_t copies) {
    using ret_t = std::vector<ss::circular_buffer<model::record_batch>>;
    return do_with(
      std::move(batches),
      ret_t(copies),
      [copies](ss::circular_buffer<model::record_batch>& batches, ret_t& data) {
          return ss::do_for_each(
                   batches,
                   [copies, &data](model::record_batch& b) mutable {
                       auto shared_batches = share_n_record_batch(
                         std::move(b), copies);

                       for (auto& buf : data) {
                           buf.push_back(std::move(shared_batches.back()));
                           shared_batches.pop_back();
                       }
                   })
            .then([&data]() mutable { return std::move(data); });
      });
} // namespace raft::details

ss::future<std::vector<model::record_batch_reader>> share_reader(
  model::record_batch_reader rdr,
  const size_t ncopies,
  const bool use_foreign_share) {
    return model::consume_reader_to_memory(std::move(rdr), model::no_timeout)
      .then([ncopies](ss::circular_buffer<model::record_batch> batches) {
          return share_n_batches(std::move(batches), ncopies);
      })
      .then([ncopies, use_foreign_share](
              std::vector<ss::circular_buffer<model::record_batch>> batches) {
          check_copy_out_of_range(ncopies, batches.size());
          std::vector<model::record_batch_reader> retval;
          retval.reserve(ncopies);
          for (auto& b : batches) {
              auto r = model::make_memory_record_batch_reader(std::move(b));
              if (use_foreign_share) {
                  r = model::make_foreign_record_batch_reader(std::move(r));
              }
              retval.emplace_back(std::move(r));
          }
          check_copy_out_of_range(ncopies, retval.size());
          return retval;
      });
}

ss::future<std::vector<model::record_batch_reader>>
foreign_share_n(model::record_batch_reader&& r, std::size_t ncopies) {
    return share_reader(std::move(r), ncopies, true);
}

ss::future<std::vector<model::record_batch_reader>>
share_n(model::record_batch_reader&& r, std::size_t ncopies) {
    return share_reader(std::move(r), ncopies, false);
}

ss::future<configuration_bootstrap_state>
read_bootstrap_state(storage::log log, ss::abort_source& as) {
    // TODO(agallego, michal) - iterate the log in reverse
    // as an optimization
    auto lstats = log.offsets();
    auto rcfg = storage::log_reader_config(
      lstats.start_offset, lstats.dirty_offset, raft_priority(), as);
    auto cfg_state = std::make_unique<configuration_bootstrap_state>();
    return log.make_reader(rcfg).then(
      [state = std::move(cfg_state)](
        model::record_batch_reader reader) mutable {
          auto raw = state.get();
          return std::move(reader)
            .consume(
              do_for_each_batch_consumer([raw](model::record_batch batch) {
                  raw->process_batch(std::move(batch));
                  return ss::make_ready_future<>();
              }),
              model::no_timeout)
            .then([s = std::move(state)]() mutable { return std::move(*s); });
      });
}

ss::future<std::optional<raft::group_configuration>>
extract_configuration(model::record_batch_reader&& reader) {
    using cfg_t = std::optional<raft::group_configuration>;
    return ss::do_with(
      std::move(reader),
      cfg_t{},
      [](model::record_batch_reader& reader, cfg_t& cfg) {
          return reader
            .consume(
              do_for_each_batch_consumer([&cfg](model::record_batch b) mutable {
                  // reader may contain different batches, skip the ones that
                  // does not have configuration
                  if (b.header().type != raft::configuration_batch_type) {
                      return ss::make_ready_future<>();
                  }
                  if (b.compressed()) {
                      return ss::make_exception_future(std::runtime_error(
                        "Compressed configuration records are "
                        "unsupported"));
                  }
                  auto it = std::prev(b.end());
                  cfg = reflection::adl<raft::group_configuration>{}.from(
                    it->share_value());
                  return ss::make_ready_future<>();
              }),
              model::no_timeout)
            .then([&cfg]() mutable { return std::move(cfg); });
      });
}
ss::circular_buffer<model::record_batch>
serialize_configuration_as_batches(group_configuration cfg) {
    auto batch = std::move(
                   storage::record_batch_builder(
                     raft::configuration_batch_type, model::offset(0))
                     .add_raw_kv(iobuf(), reflection::to_iobuf(std::move(cfg))))
                   .build();
    ss::circular_buffer<model::record_batch> batches;
    batches.reserve(1);
    batches.push_back(std::move(batch));
    return batches;
}

model::record_batch_reader serialize_configuration(group_configuration cfg) {
    return model::make_memory_record_batch_reader(
      serialize_configuration_as_batches(std::move(cfg)));
}

} // namespace raft::details
