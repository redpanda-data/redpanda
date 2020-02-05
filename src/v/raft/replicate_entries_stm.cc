#include "raft/replicate_entries_stm.h"

#include "likely.h"
#include "outcome_future_utils.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/raftgen_service.h"
#include "rpc/types.h"

#include <chrono>

namespace raft {
using namespace std::chrono_literals;

std::ostream&
operator<<(std::ostream& o, const replicate_entries_stm::retry_meta& m) {
    o << "{node: " << m.node << ", retries_left: " << m.retries_left
      << ", value: ";
    if (m.value) {
        if (m.value->has_error()) {
            auto& e = m.value->error();
            o << "{" << *m.value << ", message: " << e.message() << "}";
        } else {
            o << "[" << *m.value << "]";
        }
    } else {
        o << "nullptr";
    }
    return o << "}";
}

ss::future<result<void>> replicate_entries_stm::process_replies() {
    auto [success, failure] = partition_count();
    const auto& cfg = _ptr->_conf;
    if (success < cfg.majority()) {
        _ctxlog.info(
          "Could not get majority append_entries() including retries."
          "Replicated success:{}, failure:{}, majority:{}, nodes:{}",
          success,
          failure,
          cfg.majority(),
          cfg.nodes.size());
        return ss::make_ready_future<result<void>>(
          errc::non_majority_replication);
    }
    _ctxlog.debug(
      "Successful append with: {} acks out of: {}", success, cfg.nodes.size());
    return ss::make_ready_future<result<void>>(outcome::success());
}

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
        using reqs_t = std::vector<append_entries_request>;
        using append_res_t = storage::append_result;
        return _ptr->disk_append(std::move(req.batches))
          .then([this](append_res_t append_res) {
              return ss::make_ready_future<ret_t>(
                _ptr->make_append_entries_reply(std::move(append_res)));
          });
    }
    return send_append_entries_request(n, std::move(req));
}
ss::future<result<append_entries_reply>>
replicate_entries_stm::send_append_entries_request(
  model::node_id n, append_entries_request req) {
    using ret_t = result<append_entries_reply>;
    _ptr->update_node_hbeat_timestamp(n);
    auto shard = rpc::connection_cache::shard_for(n);
    _ctxlog.trace("Sending append entries request {} to {}", req.meta, n);
    return ss::smp::submit_to(shard, [this, n, r = std::move(req)]() mutable {
        auto& local = _ptr->_clients.local();
        if (!local.contains(n)) {
            return ss::make_ready_future<ret_t>(errc::missing_tcp_client);
        }

        return local.get(n)->get_connected().then(
          [this, r = std::move(r)](result<rpc::transport*> t) mutable {
              if (!t) {
                  return ss::make_ready_future<ret_t>(t.error());
              }
              auto timeout = raft::clock_type::now()
                             + _ptr->_jit.base_duration();
              auto f = raftgen_client_protocol(*t.value())
                         .append_entries(std::move(r), timeout);
              return wrap_exception_with_result<rpc::request_timeout_exception>(
                       errc::timeout, std::move(f))
                .then([](auto r) {
                    if (!r) {
                        return ss::make_ready_future<ret_t>(r.error());
                    }
                    return ss::make_ready_future<ret_t>(r.value().data); // copy
                });
          });
    });
}

ss::future<> replicate_entries_stm::dispatch_one(retry_meta& meta) {
    return with_gate(
             _req_bg,
             [this, &meta] {
                 return with_semaphore(
                          _sem,
                          1,
                          [this, &meta] {
                              return dispatch_retries(meta);
                          }) // semaphore
                   .then([this, &meta] {
                       if (meta.value) {
                           _ptr->process_append_reply(meta.node, *meta.value);
                       }
                   });
             })
      .handle_exception([](std::exception_ptr e) {
          raftlog.warn("Exception thrown while replicating entries - {}", e);
      });
}

ss::future<> replicate_entries_stm::dispatch_retries(retry_meta& meta) {
    return ss::do_until(
      [&meta] { return meta.finished(); },
      [this, &meta] { return dispatch_single_retry(meta); });
}

ss::future<> replicate_entries_stm::dispatch_single_retry(retry_meta& meta) {
    return share_request()
      .then([this, &meta](append_entries_request r) mutable {
          return do_dispatch_one(meta.node, std::move(r));
      })
      .handle_exception([this](const std::exception_ptr& e) {
          _ctxlog.warn("Error while replicating entries {}", e);
          return result<append_entries_reply>(
            errc::append_entries_dispatch_error);
      })
      .then([this, &meta](result<append_entries_reply> reply) mutable {
          --meta.retries_left;
          if (reply || meta.retries_left == 0) {
              meta.set_value(std::move(reply));
          }
      });
}

ss::future<result<replicate_result>> replicate_entries_stm::apply() {
    using reqs_t = append_entries_request;
    using append_res_t = std::vector<storage::append_result>;
    for (auto& n : _ptr->_conf.nodes) {
        _replies.emplace_back(n.id(), _max_retries);
    }
    for (auto& m : _replies) {
        (void)dispatch_one(m); // background
    }
    const size_t majority = _ptr->_conf.majority();
    const size_t all = _ptr->_conf.nodes.size();
    return _sem.wait(majority)
      .then([this, majority, all] {
          return ss::do_until(
            [this, majority, all] {
                auto [success, failure] = partition_count();
                _ctxlog.trace(
                  "Replicate results [success:{}, failures:{}, majority: {}]",
                  success,
                  failure,
                  majority);
                return success >= majority || failure >= majority
                       || (success + failure) >= all;
            },
            [this] {
                return ss::with_gate(_req_bg, [this] { return _sem.wait(1); });
            });
      })
      .then([this] {
          // If not cancelled even after successfull replication replicate
          // caller would have to wait for failing dispatches to exhaust replies
          // counter, after having majority of either success or failures we no
          // longer have to wait for slow retries to finish, therefore we can
          // set their reply_counter to 0
          cancel_not_finished();
      })
      .then([this] { return process_replies(); })
      .then([this](result<void> r) {
          if (!r) {
              return ss::make_ready_future<result<replicate_result>>(r.error());
          }
          // append only when we have majority
          auto m = std::find_if(
            _replies.cbegin(), _replies.cend(), [](const retry_meta& i) {
                return i.get_state() == retry_meta::state::success;
            });
          if (unlikely(m == _replies.end())) {
              throw std::runtime_error(
                "Logic error. cannot acknowledge commits");
          }
          auto last_offset = m->value->value().last_log_index;
          return _ptr->do_maybe_update_leader_commit_idx().then([last_offset] {
              return ss::make_ready_future<result<replicate_result>>(
                replicate_result{
                  .last_offset = model::offset(last_offset),
                });
          });
      });
}

ss::future<> replicate_entries_stm::wait() { return _req_bg.close(); }

replicate_entries_stm::replicate_entries_stm(
  consensus* p, int32_t max_retries, append_entries_request r)
  : _ptr(p)
  , _max_retries(max_retries)
  , _req(std::move(r))
  , _share_sem(1)
  , _sem(_ptr->_conf.nodes.size())
  , _ctxlog(_ptr->_self, raft::group_id(_ptr->_meta.group)) {}

void replicate_entries_stm::cancel_not_finished() {
    for (auto& m : _replies) {
        if (!m.finished()) {
            m.cancel();
        }
    }
}

std::pair<int32_t, int32_t> replicate_entries_stm::partition_count() const {
    int32_t success = 0;
    int32_t failure = 0;
    for (auto& m : _replies) {
        auto state = m.get_state();
        if (state == retry_meta::state::success) {
            ++success;
        } else if (state != retry_meta::state::in_progress) {
            ++failure;
        }
    }
    return {success, failure};
}

replicate_entries_stm::~replicate_entries_stm() {
    auto gate_not_closed = _req_bg.get_count() > 0 && !_req_bg.is_closed();
    auto [success, failures] = partition_count();
    if (gate_not_closed || (success + failures) != _replies.size()) {
        _ctxlog.error(
          "Must call replicate_entries_stm::wait(). success: {}, "
          "failures:{}, total:{}, is_gate_closed:{}",
          success,
          failures,
          _replies.size(),
          _req_bg.is_closed());
        std::terminate();
    }
}

} // namespace raft