#include "client.h"

#include <smf/lz4_filter.h>
#include <smf/zstd_filter.h>

// For trace only
#include <flatbuffers/minireflect.h>
#include <seastar/core/reactor.hh>

#include "filesystem/wal_segment_record.h"
#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"

namespace api {

client::txn::txn(const client_opts &_opts, client_stats *m,
                 int64_t transaction_id,
                 // ips of the actual chain
                 std::vector<uint32_t> chain,
                 seastar::shared_ptr<redpanda_api_client> c)
  : opts(_opts), rpc_(c), stats_(m) {
  data_.data->txn_id = transaction_id;
  data_.data->producer_id = opts.producer_id;
  auto &p = *data_.data.get();
  p.chain_index = 0;
  p.chain = std::move(chain);
  p.put = std::make_unique<wal_put_requestT>();
  p.put->topic = opts.topic_id;
  p.put->ns = opts.ns_id;
}

client::txn::txn(txn &&o) noexcept
  : opts(std::move(o.opts)), rpc_(std::move(o.rpc_)), data_(std::move(o.data_)),
    submitted_(std::move(o.submitted_)), stats_(std::move(o.stats_)) {}

void
client::txn::stage(const char *key, int32_t key_size, const char *value,
                   int32_t value_size) {
  LOG_THROW_IF(submitted_,
               "Transaction already submitted. Cannot stage more data");
  const int32_t partition =
    jump_consistent_hash(xxhash_32(key, key_size), opts.topic_partitions);
  auto &puts = data_.data->put->partition_puts;
  auto ctype = value_size >= opts.record_compression_value_threshold
                 ? opts.record_compression_type
                 : wal_compression_type::wal_compression_type_none;
  auto record =
    wal_segment_record::coalesce(key, key_size, value, value_size, ctype);
  stats_->bytes_sent += record->data.size();
  // OK to std::find usually small ~16
  auto it = std::find_if(puts.begin(), puts.end(), [partition](auto &tpl) {
    return partition == tpl->partition;
  });
  if (it == puts.end()) {
    auto ptr = std::make_unique<wal_put_partition_recordsT>();
    ptr->partition = partition;
    ptr->records.push_back(std::move(record));
    puts.push_back(std::move(ptr));
  } else {
    (*it)->records.push_back(std::move(record));
  }
}
/// \brief submits the actual transaction.
/// invalid after this call
seastar::future<smf::rpc_recv_typed_context<chains::chain_put_reply>>
client::txn::submit() {
  LOG_THROW_IF(submitted_,
               "Transaction already submitted. Can only submit once");
  stats_->write_rpc++;
  submitted_ = true;
  return rpc_->put(std::move(data_));
}

client::client(client_opts o) : opts(std::move(o)) {
  partition_offsets_.reserve(opts.topic_partitions);
}
seastar::future<>
client::open(seastar::ipv4_addr seed) {
  LOG_THROW_IF(rpc_ != nullptr, "Tried to re-open an existing connection. "
                                "Stopping before creating a resource leak");
  rpc_ = seastar::make_shared<redpanda_api_client>(seed);
  rpc_->incoming_filters().push_back(smf::zstd_decompression_filter());
  rpc_->incoming_filters().push_back(smf::lz4_decompression_filter());
  // Compress after 4MB regardless.
  rpc_->outgoing_filters().push_back(smf::lz4_compression_filter(1 << 22));
  // register decompression filters
  return rpc_->connect()
    .then([this]() {
      smf::rpc_typed_envelope<wal_topic_create_request> x;
      x.data->topic = opts.topic;
      x.data->ns = opts.topic_namespace;
      x.data->partitions = opts.topic_partitions;
      x.data->type = opts.topic_type;
      for (auto &kv : opts.topic_props) {
        auto p = std::make_unique<wal_topic_propertyT>();
        p->key = kv.first;
        p->value = kv.second;
        x.data->props.push_back(std::move(p));
      }
      return rpc_->create_topic(std::move(x));
    })
    .then([this](auto create_reply) {
      /// XXX(agallego) - this is where you would parse the chains
      /// and assign locally for this topic/all partitions :)
      /// XXX create should return the latest stats :)
      return seastar::make_ready_future<>();
    });
}
seastar::future<>
client::close() {
  if (rpc_) { return rpc_->stop(); }
  return seastar::make_ready_future<>();
}
seastar::future<smf::rpc_recv_typed_context<chains::chain_get_reply>>
client::consume(int32_t partition_override) {
  int32_t partition =
    partition_override >= 0
      ? partition_override
      : jump_consistent_hash(stats_.read_rpc++, opts.topic_partitions);

  return seastar::with_semaphore(partition_offsets_[partition].lock, 1, [=] {
    return consume_from_partition(partition);
  });
}
seastar::future<smf::rpc_recv_typed_context<chains::chain_get_reply>>
client::consume_from_partition(int32_t partition) {
  smf::rpc_typed_envelope<chains::chain_get_request> x;
  x.data->consumer_group_id = opts.consumer_group_id;
  x.data->get = std::make_unique<wal_get_requestT>();
  x.data->get->topic = opts.topic_id;
  x.data->get->server_validate_payload = opts.server_side_verify_payload;
  x.data->get->ns = opts.ns_id;
  x.data->get->partition = partition;
  x.data->get->offset = partition_offsets_[partition].offset;  // begin
  x.data->get->max_bytes = opts.consumer_max_read_bytes;
  return rpc_->get(std::move(x)).then([this, partition](auto r) {
    if (r) {
      stats_.bytes_read += sizeof(r.ctx->header) + r.ctx->payload.size();
      if (r.ctx->status() == 200) {
        auto &offset_ref = partition_offsets_[partition].offset;
        DLOG_THROW_IF(
          offset_ref > r->get()->next_offset(),
          "Incorrect offset manipulation. Asked to start at offset: "
          "{}, received offset: {}",
          offset_ref, r->get()->next_offset());
        DLOG_THROW_IF(partition != r->get()->partition(),
                      "Invalid partition. Expected: {}, got: {}", partition,
                      r->get()->partition());
        // guarantee forward progress
        offset_ref = std::max(offset_ref, r->get()->next_offset());
      }
    }
    return seastar::make_ready_future<decltype(r)>(std::move(r));
  });
}

client::txn
client::create_txn() {
  // XXX(agallego) - fix chain
  std::vector<uint32_t> chain;
  chain.push_back(uint32_t(2130706433) /*127.0.0.1*/);
  return txn(opts, &stats_, producer_txn_id_++, std::move(chain), rpc_);
}

}  // namespace api
