#include "raft/types.h"

#include "raft/consensus_utils.h"

namespace raft {
group_configuration::const_iterator
group_configuration::find_in_nodes(model::node_id id) const {
    return details::find_machine(cbegin(nodes), cend(nodes), id);
}
group_configuration::iterator
group_configuration::find_in_nodes(model::node_id id) {
    return details::find_machine(begin(nodes), end(nodes), id);
}

group_configuration::const_iterator
group_configuration::find_in_learners(model::node_id id) const {
    return details::find_machine(cbegin(learners), cend(learners), id);
}

group_configuration::iterator
group_configuration::find_in_learners(model::node_id id) {
    return details::find_machine(begin(learners), end(learners), id);
}

bool group_configuration::contains_broker(model::node_id id) const {
    auto find_by_id = [id](const model::broker b) { return b.id() == id; };
    return std::any_of(std::cbegin(nodes), std::cend(nodes), find_by_id)
           || std::any_of(
             std::cbegin(learners), std::cend(learners), find_by_id);
}

group_configuration::brokers_t group_configuration::all_brokers() const {
    std::vector<model::broker> all;
    all.reserve(nodes.size() + learners.size());
    std::copy(std::cbegin(nodes), std::cend(nodes), std::back_inserter(all));
    std::copy(
      std::cbegin(learners), std::cend(learners), std::back_inserter(all));
    return all;
}

std::ostream& operator<<(std::ostream& o, const append_entries_reply& r) {
    return o << "{node_id: " << r.node_id << ", group: " << r.group
             << ", term:" << r.term << ", last_log_index:" << r.last_log_index
             << ", result: " << r.result << "}";
}

std::ostream& operator<<(std::ostream& o, const vote_request& r) {
    return o << "{node_id: " << r.node_id << ", group: " << r.group
             << ", term:" << r.term << ", prev_log_index:" << r.prev_log_index
             << ", prev_log_term: " << r.prev_log_term << "}";
}
std::ostream& operator<<(std::ostream& o, const follower_index_metadata& i) {
    return o << "{node_id: " << i.node_id
             << ", last_log_idx: " << i.last_log_index
             << ", match_index: " << i.match_index
             << ", next_index: " << i.next_index
             << ", is_learner: " << i.is_learner
             << ", is_recovering: " << i.is_recovering << "}";
}

std::ostream& operator<<(std::ostream& o, const heartbeat_request& r) {
    o << "{node: " << r.node_id << ", meta: [";
    for (auto& m : r.meta) {
        o << m << ",";
    }
    return o << "]}";
}
std::ostream& operator<<(std::ostream& o, const heartbeat_reply& r) {
    o << "{meta:[";
    for (auto& m : r.meta) {
        o << m << ",";
    }
    return o << "]}";
}

std::ostream& operator<<(std::ostream& o, const group_configuration& c) {
    o << "{group_configuration: nodes: [";
    for (auto& n : c.nodes) {
        o << n.id();
    }
    o << "], learners: [";
    for (auto& n : c.learners) {
        o << n.id();
    }
    return o << "]}";
}

std::ostream& operator<<(std::ostream& o, const consistency_level& l) {
    switch (l) {
    case consistency_level::quorum_ack:
        o << "consistency_level::quorum_ack";
        break;
    case consistency_level::leader_ack:
        o << "consistency_level::leader_ack";
        break;
    case consistency_level::no_ack:
        o << "consistency_level::no_ack";
        break;
    default:
        o << "unknown consistency_level";
    }
    return o;
}
std::ostream& operator<<(std::ostream& o, const protocol_metadata& m) {
    return o << "{raft_group:" << m.group << ", commit_index:" << m.commit_index
             << ", term:" << m.term << ", prev_log_index:" << m.prev_log_index
             << ", prev_log_term:" << m.prev_log_term << "}";
}
std::ostream& operator<<(std::ostream& o, const vote_reply& r) {
    return o << "{term:" << r.term << ", vote_granted: " << r.granted
             << ", log_ok:" << r.log_ok << "}";
}
std::ostream&
operator<<(std::ostream& o, const append_entries_reply::status& r) {
    switch (r) {
    case append_entries_reply::status::success:
        o << "success";
        return o;
    case append_entries_reply::status::failure:
        o << "failure";
        return o;
    case append_entries_reply::status::group_unavailable:
        o << "group_unavailable";
        return o;
    default:
        return o << "uknown append_entries_reply::status";
    }
}
} // namespace raft

namespace reflection {

struct rpc_model_reader_consumer {
    explicit rpc_model_reader_consumer(iobuf& oref)
      : ref(oref) {}
    ss::future<ss::stop_iteration> operator()(model::record_batch batch) {
        reflection::serialize(ref, batch.header());
        if (!batch.compressed()) {
            reflection::serialize<int8_t>(ref, 0);
            for (model::record& r : batch) {
                reflection::serialize(ref, std::move(r));
            }
        } else {
            reflection::serialize<int8_t>(ref, 1);
            reflection::serialize(ref, std::move(batch).release());
        }
        return ss::make_ready_future<ss::stop_iteration>(
          ss::stop_iteration::no);
    }
    void end_of_stream(){};
    iobuf& ref;
};

ss::future<> async_adl<raft::append_entries_request>::to(
  iobuf& out, raft::append_entries_request&& request) {
    return model::consume_reader_to_memory(
             std::move(request.batches), model::no_timeout)
      .then([&out, request = std::move(request)](
              ss::circular_buffer<model::record_batch> batches) {
          reflection::adl<uint32_t>{}.to(out, batches.size());
          for (auto& batch : batches) {
              reflection::serialize(out, std::move(batch));
          }
          reflection::serialize(out, request.meta, request.node_id);
      });
}

ss::future<raft::append_entries_request>
async_adl<raft::append_entries_request>::from(iobuf_parser& in) {
    auto batchCount = reflection::adl<uint32_t>{}.from(in);
    auto batches = ss::circular_buffer<model::record_batch>{};
    batches.reserve(batchCount);
    for (uint32_t i = 0; i < batchCount; ++i) {
        batches.push_back(adl<model::record_batch>{}.from(in));
    }
    auto reader = model::make_memory_record_batch_reader(std::move(batches));
    auto meta = reflection::adl<raft::protocol_metadata>{}.from(in);
    auto n = reflection::adl<model::node_id>{}.from(in);
    auto ret = raft::append_entries_request{
      .node_id = n, .meta = std::move(meta), .batches = std::move(reader)};
    return ss::make_ready_future<raft::append_entries_request>(std::move(ret));
}

} // namespace reflection
