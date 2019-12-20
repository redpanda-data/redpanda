#include "raft/replicate_entries_stm.h"

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

future<result<void>> replicate_entries_stm::process_replies() {
    auto [success, failure] = partition_count();
    const auto& cfg = _ptr->_conf;
    if (success < cfg.majority()) {
        raftlog.info(
          "Could not get majority append_entries() including retries."
          "Replicated success:{}, failure:{}, majority:{}, nodes:{}",
          success,
          failure,
          cfg.majority(),
          cfg.nodes.size());
        return make_ready_future<result<void>>(errc::non_majority_replication);
    }
    raftlog.debug(
      "Successful append with: {} acks out of: {}", success, cfg.nodes.size());
    return make_ready_future<result<void>>(outcome::success());
}

future<std::vector<append_entries_request>>
replicate_entries_stm::share_request_n(size_t n) {
    // one extra copy is needed for retries
    return with_semaphore(_share_sem, 1, [this, n] {
        return details::foreign_share_n(std::move(_req.entries), n + 1)
          .then([this](std::vector<std::vector<raft::entry>> es) {
              // keep a copy around until the end
              _req.entries = std::move(es.back());
              es.pop_back();
              std::vector<append_entries_request> reqs;
              reqs.reserve(es.size());
              while (!es.empty()) {
                  reqs.push_back(append_entries_request{
                    _req.node_id, _req.meta, std::move(es.back())});
                  es.pop_back();
              }
              return reqs;
          });
    });
}

future<result<append_entries_reply>> replicate_entries_stm::do_dispatch_one(
  model::node_id n, append_entries_request req) {
    using ret_t = result<append_entries_reply>;

    if (n == _ptr->_self) {
        using reqs_t = std::vector<append_entries_request>;
        using append_res_t = std::vector<storage::log::append_result>;
        return _ptr->disk_append(std::move(req.entries))
          .then([this](append_res_t append_res) {
              return make_ready_future<ret_t>(_ptr->make_append_entries_reply(
                _req.meta, std::move(append_res)));
          });
    }
    auto shard = rpc::connection_cache::shard_for(n);
    return smp::submit_to(shard, [this, n, r = std::move(req)]() mutable {
        auto& local = _ptr->_clients.local();
        if (!local.contains(n)) {
            return make_ready_future<ret_t>(errc::missing_tcp_client);
        }

        return local.get(n)->get_connected().then(
          [r = std::move(r)](result<rpc::transport*> t) mutable {
              if (!t) {
                  return make_ready_future<ret_t>(t.error());
              }
              auto f = raftgen_client_protocol(*t.value())
                         .append_entries(std::move(r), rpc::no_timeout);
              return result_with_timeout(
                       raft::clock_type::now() + 1s,
                       errc::timeout,
                       std::move(f))
                .then([](auto r) {
                    if (!r) {
                        return make_ready_future<ret_t>(r.error());
                    }
                    return make_ready_future<ret_t>(r.value().data); // copy
                });
          });
    });
}

future<> replicate_entries_stm::dispatch_one(retry_meta& meta) {
    using reqs_t = std::vector<append_entries_request>;
    return with_gate(_req_bg, [this, &meta] {
        return with_semaphore(_sem, 1, [this, &meta] {
            const size_t majority = _ptr->_conf.majority();
            return do_until(
              [this, majority, &meta] {
                  // Either this request finished, or we reached majority
                  if (meta.finished()) {
                      return true;
                  }
                  auto [success, failure] = partition_count();
                  return success >= majority || failure >= majority;
              },
              [this, &meta] {
                  return share_request_n(1)
                    .then([this, &meta](reqs_t r) mutable {
                        return do_dispatch_one(meta.node, std::move(r.back()));
                    })
                    .then([this,
                           &meta](result<append_entries_reply> reply) mutable {
                        --meta.retries_left;
                        if (reply || meta.retries_left == 0) {
                            meta.set_value(std::move(reply));
                        }
                    }); // actual work
              });       // loop
        });             // semaphore
    });                 // gate
}

future<result<replicate_result>> replicate_entries_stm::apply() {
    using reqs_t = std::vector<append_entries_request>;
    using append_res_t = std::vector<storage::log::append_result>;
    for (auto& n : _ptr->_conf.nodes) {
        _replies.push_back(retry_meta(n.id(), _max_retries));
    }
    for (auto& m : _replies) {
        (void)dispatch_one(m); // background
    }

    const size_t majority = _ptr->_conf.majority();
    return _sem.wait(majority)
      .then([this, majority] {
          return do_until(
            [this, majority] {
                auto [success, failure] = partition_count();
                return success >= majority || failure >= majority;
            },
            [this] {
                return with_gate(_req_bg, [this] { return _sem.wait(1); });
            });
      })
      .then([this] { return process_replies(); })
      .then([this](result<void> r) {
          if (!r) {
              // we need to truncate on unsuccessful replication
              auto offset = model::offset(_ptr->_meta.commit_index);
              auto term = model::term_id(_ptr->_meta.term);
              raftlog.debug(
                "Truncating log for unfinished append at offset:{}, "
                "term:{}",
                offset,
                term);
              return _ptr->_log.truncate(offset, term).then([r = std::move(r)] {
                  return make_ready_future<result<replicate_result>>(r.error());
              });
          }
          // append only when we have majority
          return share_request_n(1).then([this](reqs_t r) {
              auto m = std::find_if(
                _replies.cbegin(), _replies.cend(), [](const retry_meta& i) {
                    return i.is_success();
                });
              if (__builtin_expect(m == _replies.end(), false)) {
                  throw std::runtime_error(
                    "Logic error. cannot acknowledge commits");
              }
              auto last_offset = m->value->value().last_log_index;
              return _ptr
                ->commit_entries(std::move(r.back().entries), m->value->value())
                .discard_result()
                .then([last_offset] {
                    return make_ready_future<result<replicate_result>>(
                      replicate_result{
                        .last_offset = model::offset(last_offset),
                      });
                });
          });
      });
}
future<> replicate_entries_stm::wait() {
    return _req_bg.close();
    // TODO(agallego) - propagate the entries replies to
    // _ptr->process_heartbeat() for all replies
}

replicate_entries_stm::replicate_entries_stm(
  consensus* p, int32_t max_retries, append_entries_request r)
  : _ptr(p)
  , _max_retries(max_retries)
  , _req(std::move(r))
  , _share_sem(1)
  , _sem(_ptr->_conf.nodes.size()) {}

std::pair<int32_t, int32_t> replicate_entries_stm::partition_count() const {
    int32_t success = 0;
    int32_t failure = 0;
    for (auto& m : _replies) {
        if (m.is_success()) {
            ++success;
        } else if (m.is_failure()) {
            ++failure;
        }
    }
    return {success, failure};
}

replicate_entries_stm::~replicate_entries_stm() {
    auto gate_not_closed = _req_bg.get_count() > 0 && !_req_bg.is_closed();
    auto [success, failures] = partition_count();
    if (gate_not_closed || (success + failures) != _replies.size()) {
        raftlog.error(
          "Must call replicate_entries_stm::wait(). success{}, "
          "failures:{}, total:{}, is_gate_closed:{}",
          success,
          failures,
          _replies.size(),
          _req_bg.is_closed());
        std::terminate();
    }
}

} // namespace raft
