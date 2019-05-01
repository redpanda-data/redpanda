#include "wal_requests.h"

#include "hashing/jump_consistent_hash.h"
#include "hashing/xx.h"

#include "wal_name_extractor_utils.h"

namespace v {

bool
wal_create_request::is_valid(const wal_create_request &r) {
  try {
    return r.req->topic() &&
           wal_name_extractor_utils::is_valid_ns_topic_name(
             r.req->topic()->c_str()) &&
           r.req->ns() &&
           wal_name_extractor_utils::is_valid_ns_topic_name(
             r.req->ns()->c_str()) &&
           r.req->partitions() <= 1024 /*artificial limit*/
           && (r.req->ns()->size() + r.req->topic()->size()) <
                200 /*most linux filesystems have a 255 char limit*/;
  } catch (const std::exception &e) {
    LOG_ERROR("Validating wal_create_request: caught exception: {}", e.what());
  }
  return false;
}

bool
wal_read_request::is_valid(const wal_read_request &r) {
  try {
    // traversing the fbs might throw
    (void)r.req->partition();
    (void)r.req->offset();
    (void)r.req->max_bytes();
    (void)r.req->topic();
    (void)r.req->ns();
  } catch (const std::exception &e) {
    LOG_ERROR("Validating wal_read_request: caught exception: {}", e.what());
    return false;
  }
  return true;
}

wal_read_reply::wal_read_reply(int64_t ns, int64_t topic, int32_t partition,
                               int64_t request_offset, bool validate_chksm)
  : validate_checksum(validate_chksm) {
  data_.next_offset = request_offset;
  data_.partition = partition;
  data_.ns = ns;
  data_.topic = topic;
  data_.error = wal_read_errno::wal_read_errno_none;
}

void
wal_write_reply::set_reply_partition_tuple(int64_t ns, int64_t topic,
                                           int32_t partition, int64_t begin,
                                           int64_t end) {
  DLOG_THROW_IF(ns != this->ns, "missmatching namespace");
  DLOG_THROW_IF(topic != this->topic, "missmatching topic");

  wal_nstpidx key(ns, topic, partition);
  auto it = cache_.find(key);
  if (it == cache_.end()) {
    auto p = std::make_unique<wal_put_reply_partition_tupleT>();
    p->partition = partition;
    p->start_offset = begin;
    p->end_offset = end;
    cache_.insert({std::move(key), std::move(p)});
  } else {
    it->second->start_offset = std::max(it->second->start_offset, begin);
    it->second->end_offset = std::max(it->second->end_offset, end);
  }
}

std::unique_ptr<wal_put_replyT>
wal_write_reply::release() {
  auto ret = std::make_unique<wal_put_replyT>();
  ret->ns = ns;
  ret->topic = topic;
  ret->error = err;
  for (auto &&p : cache_) {
    ret->offsets.push_back(std::move(p.second));
  }
  return ret;
}

}  // namespace v
