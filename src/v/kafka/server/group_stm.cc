#include "kafka/server/group_stm.h"

namespace kafka {

void group_stm::overwrite_metadata(group_log_group_metadata&& metadata) {
    _metadata = std::move(metadata);
    _is_loaded = true;
}

void group_stm::remove_offset(model::topic_partition key) {
    _offsets.erase(key);
}

void group_stm::update_offset(
  model::topic_partition key,
  model::offset offset,
  group_log_offset_metadata&& meta) {
    _offsets[key] = logged_metadata{
      .log_offset = offset, .metadata = std::move(meta)};
}

std::ostream& operator<<(std::ostream& os, const group_log_offset_key& key) {
    fmt::print(
      os,
      "group {} topic {} partition {}",
      key.group(),
      key.topic(),
      key.partition());
    return os;
}

std::ostream&
operator<<(std::ostream& os, const group_log_offset_metadata& md) {
    fmt::print(os, "offset {}", md.offset());
    return os;
}

} // namespace kafka
