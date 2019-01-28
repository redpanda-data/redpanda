#include "client_opts.h"

#include "hashing/xx.h"

namespace rp {
namespace api {

client_opts::client_opts(seastar::sstring _topic_namespace,
                         seastar::sstring _topic, int64_t _consumer_group_id,
                         int64_t _producer_id)
  : topic_namespace(std::move(_topic_namespace)), topic(std::move(_topic)),
    consumer_group_id(_consumer_group_id), producer_id(_producer_id) {
  ns_id = rp::xxhash_64(topic_namespace.c_str(), topic_namespace.size());
  topic_id = rp::xxhash_64(topic.c_str(), topic.size());
}

}  // namespace api
}  // namespace rp
