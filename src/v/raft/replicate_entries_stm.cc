#include "raft/replicate_entries_stm.h"

#include "raft/consensus_utils.h"
#include "raft/logger.h"

#include <chrono>

namespace raft {
using namespace std::chrono_literals;
future<> replicate_entries_stm::process_replies() {
    auto [success, failure] = partition_count();
    const auto& cfg = _ptr->_conf;
    if (success < cfg.majority()) {
        return make_exception_future<>(std::runtime_error(fmt::format(
          "Could not get majority append_entries() including retries."
          "Replicated entries:{}, majority:{}, nodes:{}",
          success,
          cfg.majority(),
          cfg.nodes.size())));
    }
    raftlog.debug(
      "Successful append with: {} acks out of: {}", success, cfg.nodes.size());
    return make_ready_future<>();
}

future<std::vector<append_entries_request>>
replicate_entries_stm::share_request_n(size_t n) {
    // one extra copy is needed for retries
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
}

future<append_entries_reply> replicate_entries_stm::do_dispatch_one(
  model::node_id n, append_entries_request req) {
    if (n == _ptr->_self) {
        using reqs_t = std::vector<append_entries_request>;
        using append_res_t = std::vector<storage::log::append_result>;
        return _ptr->disk_append(std::move(req.entries))
          .then([this](append_res_t append_res) {
              return _ptr->make_append_entries_reply(
                _req.meta, std::move(append_res));
          });
    }
    auto shard = client_cache::shard_for(n);
    return smp::submit_to(shard, [this, n, r = std::move(req)]() mutable {
        auto& local = _ptr->_clients.local();
        if (!local.contains(n)) {
            return make_exception_future<append_entries_reply>(
              std::runtime_error(
                fmt::format("Missing node {} in client_cache", n)));
        }
        return local.get(n)->with_client(
          [r = std::move(r)](reconnect_client::client_type& cli) mutable {
              auto f = cli.append_entries(std::move(r));
              return with_timeout(raft::clock_type::now() + 3s, std::move(f))
                .then([](rpc::client_context<append_entries_reply> r) {
                    return r.data; // copy
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
                  return meta.finished() || _ongoing.size() < majority;
              },
              [this, &meta] {
                  return share_request_n(1)
                    .then([this, &meta](reqs_t r) mutable {
                        return do_dispatch_one(meta.node, std::move(r.back()));
                    })
                    .then_wrapped(
                      [this,
                       &meta](future<append_entries_reply> reply) mutable {
                          --meta.retries_left;
                          try {
                              meta.value = std::move(reply.get0());
                              meta.retries_left = 0;
                          } catch (...) {
                              raftlog.info(
                                "Could not replicate entry due to: {}, retries "
                                "left:{}",
                                std::current_exception(),
                                meta.retries_left);
                          }
                          if (meta.retries_left <= 0) {
                              _ongoing.erase(_ongoing.iterator_to(meta));
                          }
                      });
              });
        });
    });
}

future<> replicate_entries_stm::apply() {
    using reqs_t = std::vector<append_entries_request>;
    using append_res_t = std::vector<storage::log::append_result>;
    return share_request_n(1).then([this](reqs_t r) {
        for (retry_meta& m : _ongoing) {
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
          .then([this] {
              // append only when we have majority
              return share_request_n(1).then([this](reqs_t r) {
                  return _ptr
                    ->commit_entries(
                      std::move(r.back().entries),
                      std::move(_replies.back()->value.value()))
                    .discard_result();
              });
          });
    });
}
future<> replicate_entries_stm::wait() { return _req_bg.close(); }

replicate_entries_stm::replicate_entries_stm(
  consensus* p, int32_t max_retries, append_entries_request r)
  : _ptr(p)
  , _req(std::move(r))
  , _sem(_ptr->_conf.nodes.size()) {
    for (auto& n : _ptr->_conf.nodes) {
        _replies.push_back(std::make_unique<retry_meta>(n.id(), max_retries));
        _ongoing.push_back(*_replies.back());
    }
}
std::pair<int32_t, int32_t> replicate_entries_stm::partition_count() const {
    int32_t success = std::accumulate(
      _replies.cbegin(),
      _replies.cend(),
      int32_t(0),
      [](int32_t acc, const meta_ptr& m) { return m->is_success(); });
    return {success, _replies.size() - success};
}
replicate_entries_stm::~replicate_entries_stm() {
    if (!_req_bg.is_closed() || !_ongoing.empty()) {
        raftlog.error(
          "Must call replicate_entries_stm::wait(). Missing futures:{}",
          _ongoing.size());
        std::terminate();
    }
}

} // namespace raft
