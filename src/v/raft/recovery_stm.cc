#include "raft/recovery_stm.h"

#include "outcome_future_utils.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"

#include <seastar/core/future-util.hh>

#include <chrono>

namespace raft {
using namespace std::chrono_literals;

recovery_stm::recovery_stm(
  consensus* p, follower_index_metadata& meta, seastar::io_priority_class prio)
  : _ptr(p)
  , _meta(meta)
  , _prio(prio) {}

seastar::future<> recovery_stm::do_one_read() {
    storage::log_reader_config cfg{
      .start_offset = _meta.commit_index,
      .max_bytes = 1024 * 1024, // 1MB
      .min_bytes = 1,           // we know at least 1 entry
      .prio = _prio,
      .type_filter = {},
      .max_offset = model::offset(_ptr->_meta.commit_index) // inclusive
    };
    return do_with(
             _ptr->_log.make_reader(cfg),
             [this](model::record_batch_reader& reader) {
                 return reader.consume(
                   details::memory_batch_consumer(), model::no_timeout);
             })
      .then([](std::vector<model::record_batch> batches) {
          // wrap in a foreign core destructor
          return details::foreign_share_n(
            details::batches_as_entries(std::move(batches)), 1);
      })
      .then([this](std::vector<std::vector<raft::entry>> es) {
          return replicate(std::move(es.back()));
      });
}

seastar::future<> recovery_stm::replicate(std::vector<raft::entry> es) {
    using ret_t = result<append_entries_reply>;
    auto shard = client_cache::shard_for(_meta.node_id);
    // TODO(agallego) - verify we shouldn't use 'this->_meta' instead of _ptr
    auto r = append_entries_request{
      .node_id = _meta.node_id, .meta = _ptr->_meta, .entries = std::move(es)};
    return smp::submit_to(
             shard,
             [this, r = std::move(r)]() mutable {
                 auto& local = _ptr->_clients.local();
                 if (!local.contains(_meta.node_id)) {
                     return make_ready_future<ret_t>(errc::missing_tcp_client);
                 }
                 return local.get(_meta.node_id)
                   ->get_connected()
                   .then(
                     [r = std::move(r)](result<rpc::transport*> cli) mutable {
                         if (!cli) {
                             return make_ready_future<ret_t>(cli.error());
                         }
                         auto f = raftgen_client_protocol(*cli.value())
                                    .append_entries(std::move(r));
                         return result_with_timeout(
                                  raft::clock_type::now() + 1s,
                                  errc::timeout,
                                  std::move(f))
                           .then([](auto r) {
                               if (!r) {
                                   return make_ready_future<ret_t>(r.error());
                               }
                               return make_ready_future<ret_t>(r.value().data);
                           });
                     });
             })
      .then([this](auto r) {
          if (!r || !r.value().success) {
              raftlog.error(
                "recovery_stm: not replicate entry: {} - {}",
                r,
                r.error().message());
              _stop_requested = true;
              return;
          }
          append_entries_reply reply = std::move(r.value());
          _meta.commit_index = model::offset(reply.last_log_index);
          _meta.term = model::term_id(reply.term);
      });
}
seastar::future<> recovery_stm::apply() {
    return seastar::do_until(
             [this] {
                 return _meta.commit_index == _ptr->_meta.commit_index
                        || _stop_requested;
             },
             [this] { return do_one_read(); })
      .finally([this] { _meta.is_recovering = false; });
}
} // namespace raft
