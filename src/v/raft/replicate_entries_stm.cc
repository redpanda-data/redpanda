#include "raft/replicate_entries_stm.h"

#include "raft/consensus_utils.h"
#include "raft/logger.h"

#include <chrono>

namespace raft {
using namespace std::chrono_literals;
future<> replicate_entries_stm::process_replies() {
    const size_t success = std::accumulate(
      _replies.cbegin(),
      _replies.cend(),
      size_t(0),
      [](size_t acc, const meta_ptr& m) {
          return m->value && m->value->success ? acc + 1 : acc;
      });
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
        return _ptr->do_append_entries(std::move(req));
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
                  [this, &meta](future<append_entries_reply> reply) mutable {
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
}

future<> replicate_entries_stm::apply() {
    for (retry_meta& m : _ongoing) {
        (void)dispatch_one(m); // background
    }
    const size_t majority = _ptr->_conf.majority();
    return _sem.wait(majority).then([this] { return process_replies(); });
}
future<> replicate_entries_stm::wait() {
    const size_t majority = _ptr->_conf.majority();
    return _sem.wait(_ptr->_conf.nodes.size() - majority);
}

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
replicate_entries_stm::~replicate_entries_stm() {
    if (!_ongoing.empty()) {
        raftlog.error(
          "Must call replicate_entries_stm::wait(). Missing futures:{}",
          _ongoing.size());
        std::terminate();
    }
}

} // namespace raft
