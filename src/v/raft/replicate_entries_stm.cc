#include "raft/replicate_entries_stm.h"

#include "likely.h"
#include "model/fundamental.h"
#include "model/metadata.h"
#include "outcome_future_utils.h"
#include "raft/consensus.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/raftgen_service.h"
#include "raft/types.h"
#include "rpc/types.h"

#include <chrono>

namespace raft {
using namespace std::chrono_literals;

ss::future<append_entries_request> replicate_entries_stm::share_request() {
    // one extra copy is needed for retries
    return with_semaphore(_share_sem, 1, [this] {
        return details::foreign_share_n(std::move(_req.batches), 2)
          .then([this](std::vector<model::record_batch_reader> readers) {
              // keep a copy around until the end
              _req.batches = std::move(readers.back());
              readers.pop_back();
              return append_entries_request{
                _req.node_id, _req.meta, std::move(readers.back())};
          });
    });
}
ss::future<result<append_entries_reply>> replicate_entries_stm::do_dispatch_one(
  model::node_id n, append_entries_request req) {
    using ret_t = result<append_entries_reply>;

    if (n == _ptr->_self) {
        auto f = ss::with_semaphore(
          _ptr->_op_sem, 1, [this, req = std::move(req)]() mutable {
              return _ptr->flush_log()
                .then([this]() {
                    append_entries_reply reply;
                    reply.node_id = _ptr->_self;
                    reply.group = _ptr->_meta.group;
                    reply.term = _ptr->_meta.term;
                    reply.last_log_index = _ptr->_log.max_offset();
                    reply.result = append_entries_reply::status::success;
                    return ret_t(std::move(reply));
                })
                .handle_exception(
                  []([[maybe_unused]] const std::exception_ptr& ex) {
                      return ret_t(errc::leader_flush_failed);
                  });
          });
        _dispatch_sem.signal();
        return f;
    }
    return send_append_entries_request(n, std::move(req));
}

clock_type::time_point replicate_entries_stm::append_entries_timeout() {
    return raft::clock_type::now() + _ptr->_replicate_append_timeout;
}

ss::future<result<append_entries_reply>>
replicate_entries_stm::send_append_entries_request(
  model::node_id n, append_entries_request req) {
    _ptr->update_node_hbeat_timestamp(n);
    _ctxlog.trace("Sending append entries request {} to {}", req.meta, n);

    auto f = _ptr->_client_protocol.append_entries(
      n, std::move(req), rpc::client_opts(append_entries_timeout()));
    _dispatch_sem.signal();
    return f;
}

ss::future<> replicate_entries_stm::dispatch_one(model::node_id id) {
    return ss::with_gate(
             _req_bg,
             [this, id] {
                 return dispatch_single_retry(id).then(
                   [this, id](result<append_entries_reply> reply) {
                       _ptr->process_append_entries_reply(id, reply);
                   });
             })
      .handle_exception_type([](const ss::gate_closed_exception&) {});
}

ss::future<result<append_entries_reply>>
replicate_entries_stm::dispatch_single_retry(model::node_id id) {
    return share_request()
      .then([this, id](append_entries_request r) mutable {
          return do_dispatch_one(id, std::move(r));
      })
      .handle_exception([this](const std::exception_ptr& e) {
          _ctxlog.warn("Error while replicating entries {}", e);
          return result<append_entries_reply>(
            errc::append_entries_dispatch_error);
      });
}

ss::future<storage::append_result> replicate_entries_stm::append_to_self() {
    return share_request().then([this](append_entries_request req) mutable {
        _ctxlog.trace("Self append entries - {}", req.meta);
        return _ptr->disk_append(std::move(req.batches));
    });
}

inline bool replicate_entries_stm::is_follower_recovering(model::node_id id) {
    return id != _ptr->self() && _ptr->_fstats.get(id).is_recovering;
}

ss::future<result<replicate_result>>
replicate_entries_stm::apply(ss::semaphore_units<> u) {
    // first append lo leader log, no flushing
    return append_to_self()
      .then(
        [this, u = std::move(u)](storage::append_result append_result) mutable {
            // dispatch requests to followers & leader flush
            uint16_t requests_count = 0;
            for (auto& n : _ptr->_conf.nodes) {
                // We are not dispatching request to followers that are
                // recovering
                if (is_follower_recovering(n.id())) {
                    _ctxlog.trace(
                      "Skipping sending append request to {}, recovering",
                      n.id());
                    continue;
                }
                ++requests_count;
                (void)dispatch_one(n.id()); // background
            }
            // Wait until all RPCs will be dispatched
            return _dispatch_sem.wait(requests_count)
              .then([u = std::move(u), append_result]() mutable {
                  return append_result;
              });
        })
      .then([this](storage::append_result append_result) {
          // this is happening outside of _opsem
          // store offset and term of an appended entry
          auto appended_offset = append_result.last_offset;
          auto appended_term = append_result.last_term;

          auto stop_cond = [this, appended_offset, appended_term] {
              return _ptr->_meta.commit_index >= appended_offset
                     || appended_term != _ptr->_meta.term;
          };
          return _ptr->_commit_index_updated.wait(stop_cond).then(
            [this, appended_offset, appended_term] {
                return process_result(appended_offset, appended_term);
            });
      });
}

result<replicate_result> replicate_entries_stm::process_result(
  model::offset appended_offset, model::term_id appended_term) {
    using ret_t = result<replicate_result>;
    _ctxlog.trace(
      "Replication result [offset: {}, term: {}, commit_idx: "
      "{}, "
      "current_term: {}]",
      appended_offset,
      appended_term,
      _ptr->_meta.commit_index,
      _ptr->_meta.term);

    // if term has changed we have to check if entry was
    // replicated
    if (appended_term != _ptr->_meta.term) {
        if (_ptr->_log.get_term(appended_offset) != appended_term) {
            return ret_t(errc::replicated_entry_truncated);
        }
    }
    _ctxlog.trace(
      "Replication success, last offset: {}, term: {}",
      appended_offset,
      appended_term);
    return ret_t(
      replicate_result{.last_offset = model::offset(appended_offset)});
}

ss::future<> replicate_entries_stm::wait() { return _req_bg.close(); }

replicate_entries_stm::replicate_entries_stm(
  consensus* p, append_entries_request r)
  : _ptr(p)
  , _req(std::move(r))
  , _share_sem(1)
  , _ctxlog(_ptr->_self, raft::group_id(_ptr->_meta.group)) {}

replicate_entries_stm::~replicate_entries_stm() {
    auto gate_not_closed = _req_bg.get_count() > 0 && !_req_bg.is_closed();
    if (gate_not_closed) {
        _ctxlog.error(
          "Must call replicate_entries_stm::wait(). is_gate_closed:{}",
          _req_bg.is_closed());
        std::terminate();
    }
}

} // namespace raft
