#include "raft/vote_stm.h"

#include "outcome_future_utils.h"
#include "raft/consensus_utils.h"
#include "raft/errc.h"
#include "raft/logger.h"
#include "raft/raftgen_service.h"

namespace raft {
std::ostream& operator<<(std::ostream& o, const vote_stm::vmeta& m) {
    o << "{node: " << m.node << ", value: ";
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
vote_stm::vote_stm(consensus* p)
  : _ptr(p)
  , _sem(_ptr->_conf.nodes.size())
  , _ctxlog(_ptr->_self, raft::group_id(_ptr->_meta.group)) {}

vote_stm::~vote_stm() {
    if (_vote_bg.get_count() > 0 && !_vote_bg.is_closed()) {
        _ctxlog.error("Must call vote_stm::wait()");
        std::terminate();
    }
}
ss::future<result<vote_reply>> vote_stm::do_dispatch_one(model::node_id n) {
    using ret_t = result<vote_reply>;

    _ctxlog.trace("Sending vote request to {}", n);
    auto shard = rpc::connection_cache::shard_for(n);
    return ss::smp::submit_to(shard, [this, n]() mutable {
        auto& local = _ptr->_clients.local();
        if (!local.contains(n)) {
            return ss::make_ready_future<ret_t>(errc::missing_tcp_client);
        }
        // make a local copy of `this->_req`
        return local.get(n)->get_connected().then(
          [this, r = _req, n](result<rpc::transport*> t) mutable {
              if (!t) {
                  return ss::make_ready_future<ret_t>(t.error());
              }
              auto tout = clock_type::now() + _ptr->_jit.base_duration();
              auto f
                = raftgen_client_protocol(*t.value()).vote(std::move(r), tout);
              return wrap_exception_with_result<rpc::request_timeout_exception>(
                       errc::timeout, std::move(f))
                .then([n, this](auto r) {
                    if (!r) {
                        _ctxlog.info(
                          "Error {} getting vote from {}", r.error(), n);
                        return ss::make_ready_future<ret_t>(r.error());
                    }
                    _ctxlog.trace(
                      "Got vote response {} from {}", r.value().data, n);
                    return ss::make_ready_future<ret_t>(r.value().data); // copy
                });
          });
    });
}
ss::future<> vote_stm::dispatch_one(model::node_id n) {
    return with_gate(_vote_bg, [this, n] {
        return with_semaphore(_sem, 1, [this, n] {
            if (n == _ptr->_self) {
                // skip self vote
                return ss::make_ready_future<>();
            }
            return do_dispatch_one(n).then_wrapped(
              [this, n](ss::future<result<vote_reply>> f) {
                  auto voter_reply = std::find_if(
                    _replies.begin(), _replies.end(), [n](vmeta& mi) {
                        return mi.node == n;
                    });
                  try {
                      auto r = f.get0();
                      voter_reply->set_value(std::move(r));
                  } catch (...) {
                      voter_reply->set_value(errc::vote_dispatch_error);
                  }
                  if (!voter_reply->value) {
                      _ctxlog.info("error voting: {}", *voter_reply);
                  }
              });
        });
    });
}

std::pair<int32_t, int32_t> vote_stm::partition_count() const {
    int32_t success = 0;
    int32_t failure = 0;
    for (auto& m : _replies) {
        auto voting_state = m.get_state();
        if (voting_state == vmeta::state::vote_granted) {
            ++success;
        } else if (voting_state != vmeta::state::in_progress) {
            ++failure;
        }
    }
    return {success, failure};
}
ss::future<> vote_stm::vote() {
    return with_semaphore(
             _ptr->_op_sem,
             1,
             [this] {
                 auto& m = _ptr->_meta;
                 // 5.2.1
                 _ptr->_vstate = consensus::vote_state::candidate;
                 _ptr->_leader_id = std::nullopt;
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
                 // we have to self vote before dispatching vote request to
                 // other nodes, this vote has to be done under op semaphore as
                 // it changes voted_for state
                 return self_vote();
             })
      .then([this] { return do_vote(); });
}
ss::future<> vote_stm::do_vote() {
    auto& cfg = _ptr->_conf;

    // 5.2.1. 3
    vote_reply reply;
    reply.term = _req.term;
    reply.log_ok = true;

    for (auto& n : cfg.nodes) {
        (void)dispatch_one(n.id()); // background
    }
    // wait until majority or all
    const size_t majority = cfg.majority();
    const size_t all = cfg.nodes.size();
    return _sem.wait(majority)
      .then([this, majority, all] {
          return ss::do_until(
            [this, majority, all] {
                auto [success, failure] = partition_count();
                _ctxlog.trace(
                  "Vote results [success:{}, failures:{}, majority: {}]",
                  success,
                  failure,
                  majority);
                return success >= majority || failure >= majority
                       || (success + failure) >= all;
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
ss::future<> vote_stm::wait() { return _vote_bg.close(); }

ss::future<> vote_stm::process_replies() {
    const size_t majority = _ptr->_conf.majority();
    auto [success, failure] = partition_count();
    if (_ptr->_vstate != consensus::vote_state::candidate) {
        _ctxlog.info(
          "No longer a candidate, ignoring vote replies: {}/{}",
          success,
          _ptr->_conf.nodes.size());
        return ss::make_ready_future<>();
    }
    if (success < majority) {
        _ctxlog.info(
          "Majority vote failed. {}/{} votes, need:{}",
          success,
          _ptr->_conf.nodes.size(),
          majority);
        _ptr->_vstate = consensus::vote_state::follower;
        return ss::make_ready_future<>();
    }
    std::vector<model::node_id> acks;
    acks.reserve(success);
    for (auto& r : _replies) {
        if (r.get_state() == vmeta::state::vote_granted) {
            acks.emplace_back(r.node);
        }
    }
    _ctxlog.info("vote acks in term {} from: {}", _ptr->_meta.term, acks);
    // section vote:5.2.2
    _ptr->_vstate = consensus::vote_state::leader;
    _ptr->_leader_id = _ptr->self();

    _ctxlog.info("became the leader term:{}", _ptr->_meta.term);

    return replicate_config_as_new_leader().finally(
      [this] { _ptr->_leader_notification(group_id(_ptr->_meta.group)); });
}

ss::future<> vote_stm::replicate_config_as_new_leader() {
    return _ptr->replicate_configuration(_ptr->_conf);
}

ss::future<> vote_stm::self_vote() {
    vote_reply reply;
    reply.term = _req.term;
    reply.log_ok = true;
    reply.granted = true;

    _ctxlog.trace("Voting for self in term {}", _req.term);
    _ptr->_voted_for = _ptr->_self;
    return details::persist_voted_for(
             _ptr->voted_for_filename(),
             {_ptr->_self, model::term_id(_req.term)})
      .then([this, reply = std::move(reply)] {
          auto m = std::find_if(
            _replies.begin(), _replies.end(), [this](vmeta& mi) {
                return mi.node == _ptr->_self;
            });
          m->set_value(std::move(reply));
      });
}
} // namespace raft
