#include "raft/vote_stm.h"

#include "raft/consensus_utils.h"
#include "raft/logger.h"

namespace raft {

std::ostream& operator<<(std::ostream& o, const vote_stm::vmeta& m) {
    o << "{node: " << m.node << ", is_error: " << m.is_error()
      << ", is_reply: " << m.is_reply();
    seastar::visit(m.value, [&](auto& v) { o << ", value: " << v; });
    return o << "}";
}
vote_stm::vote_stm(consensus* p)
  : _ptr(p)
  , _sem(_ptr->_conf.nodes.size()) {}
vote_stm::~vote_stm() {
    if (_vote_bg.get_count() > 0 && !_vote_bg.is_closed()) {
        raftlog.error("Must call vote_stm::wait()");
        std::terminate();
    }
}
future<vote_reply> vote_stm::do_dispatch_one(model::node_id n) {
    if (n == _ptr->_self) {
        // 5.2.1. 3
        vote_reply reply;
        reply.term = _req.term;
        reply.log_ok = true;
        reply.granted = true;
        if (_ptr->_voted_for != _ptr->_self) {
            return with_semaphore(
              _ptr->_op_sem, 1, [this, n, reply = std::move(reply)] {
                  _ptr->_voted_for = n;
                  return details::persist_voted_for(
                           _ptr->voted_for_filename(),
                           {_ptr->_self, model::term_id(_req.term)})
                    .then([reply = std::move(reply)] {
                        return make_ready_future<vote_reply>(std::move(reply));
                    });
              });
        }
        return make_ready_future<vote_reply>(std::move(reply));
    }
    auto shard = raft::client_cache::shard_for(n);
    return smp::submit_to(shard, [this, n]() mutable {
        auto& local = _ptr->_clients.local();
        if (!local.contains(n)) {
            return make_exception_future<vote_reply>(std::runtime_error(
              fmt::format("Missing node {} in client_cache", n)));
        }
        // make a local copy of `this->_req`
        return local.get(n)->with_client(
          [this, r = _req](reconnect_client::client_type& cli) mutable {
              auto f = cli.vote(std::move(r));
              auto tout = raft::clock_type::now() + _ptr->_jit.base_duration();
              return with_timeout(tout, std::move(f))
                .then([](rpc::client_context<vote_reply> r) {
                    return r.data; // copy
                });
          });
    });
}
future<> vote_stm::dispatch_one(model::node_id n) {
    return with_gate(_vote_bg, [this, n] {
        return with_semaphore(_sem, 1, [this, n] {
            return do_dispatch_one(n).then_wrapped(
              [this, n](future<vote_reply> f) {
                  auto m = std::find_if(
                    _replies.begin(), _replies.end(), [n](vmeta& mi) {
                        return mi.node == n;
                    });
                  try {
                      vote_reply r = std::move(f.get0());
                      m->value.emplace<vote_reply>(std::move(r));
                  } catch (
                    const reconnect_client::disconnected_client_exception& e) {
                      sstring tmp = fmt::format("{}", e);
                      raftlog.info("node {} is unreachable {}", n, tmp);
                      m->value.emplace<sstring>(std::move(tmp));
                  } catch (...) {
                      sstring tmp = fmt::format("{}", std::current_exception());
                      raftlog.error("Unknown error from node {} : {}", n, tmp);
                      m->value.emplace<sstring>(std::move(tmp));
                  }
              });
        });
    });
}

std::pair<int32_t, int32_t> vote_stm::partition_count() const {
    int32_t success = std::accumulate(
      _replies.cbegin(),
      _replies.cend(),
      int32_t(0),
      [](int32_t acc, const vmeta& m) {
          return m.is_vote_granted_reply() ? acc + 1 : acc;
      });
    return {success, _replies.size() - success};
}
future<> vote_stm::vote() {
    return with_semaphore(
             _ptr->_op_sem,
             1,
             [this] {
                 auto& m = _ptr->_meta;
                 // 5.2.1
                 _ptr->_vstate = consensus::vote_state::candidate;

                 // 5.2.1.2
                 m.term = m.term + 1;

                 // vote is the only method under _op_sem
                 for (auto& n : _ptr->_conf.nodes) {
                     _replies.push_back(vmeta(n.id()));
                 }
                 _req = vote_request{_ptr->_self(),
                                     m.group,
                                     m.term,
                                     m.prev_log_index,
                                     m.prev_log_term};
             })
      .then([this] { return do_vote(); });
}
future<> vote_stm::do_vote() {
    auto& cfg = _ptr->_conf;
    for (auto& n : cfg.nodes) {
        (void)dispatch_one(n.id()); // background
    }
    // wait until majority or all
    const size_t majority = cfg.majority();
    return _sem.wait(majority)
      .then([this, majority] {
          return do_until(
            [this, majority] {
                auto [success, failure] = partition_count();
                return success >= majority || failure >= majority;
            },
            [this] {
                return with_gate(_vote_bg, [this] { return _sem.wait(1); });
            });
      })
      // porcess results
      .then([this]() {
          return with_semaphore(
            _ptr->_op_sem, 1, [this] { return process_replies(); });
      });
}
future<> vote_stm::wait() { return _vote_bg.close(); }

future<> vote_stm::process_replies() {
    const size_t majority = _ptr->_conf.majority();
    auto [success, failure] = partition_count();
    if (_ptr->_vstate != consensus::vote_state::candidate) {
        raftlog.info(
          "No longer a candidate, ignoring vote replies: {}/{}",
          success,
          _ptr->_conf.nodes.size());
        return make_ready_future<>();
    }
    if (success < majority) {
        raftlog.info(
          "Majority vote failed. {}/{} votes, need:{}",
          success,
          _ptr->_conf.nodes.size(),
          majority);
        _ptr->_vstate = consensus::vote_state::follower;
        return make_ready_future<>();
    }
    std::vector<model::node_id> acks;
    acks.reserve(success);
    for (auto& r : _replies) {
        if (r.is_vote_granted_reply()) {
            acks.emplace_back(r.node);
        }
    }
    raftlog.info("vote acks from: {}", acks);
    // section vote:5.2.2
    _ptr->_conf.leader_id = _ptr->_self;
    _ptr->_vstate = consensus::vote_state::leader;

    raftlog.info(
      "became the leader for group {}, term:{}",
      _ptr->_meta.group,
      _ptr->_meta.term);

    return replicate_config_as_new_leader().finally(
      [this] { _ptr->_leader_notification(group_id(_ptr->_meta.group)); });
}

future<> vote_stm::replicate_config_as_new_leader() {
    return _ptr->replicate_configuration(_ptr->_conf);
}
} // namespace raft
