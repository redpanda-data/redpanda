#include "wal_pretty_print_utils.h"

#include <flatbuffers/minireflect.h>

namespace std {
ostream &
operator<<(ostream &o, const wal_header &h) {
  // Do not convert enum::compression to string if untrusted/uninitialized mem
  // as it will cause a sigsev since the enum size is not checked in flatbuffers
  o << "wal_header={ key_size=" << h.key_size()
    << ", value_size=" << h.value_size() << ", checksum=" << h.checksum()
    << ", time_millis=" << h.time_millis() << ", version=" << h.version()
    << ", compression=" << h.compression() << ", magic=" << h.magic() << "}";
  return o;
}

ostream &
operator<<(ostream &o, const wal_get_request &r) {
  o << "wal_get_request={ ns=" << r.ns() << ", topic=" << r.topic()
    << ", partition=" << r.partition() << ", offset=" << r.offset()
    << ", max_bytes=" << r.max_bytes() << " } ";
  return o;
}
ostream &
operator<<(ostream &o, const wal_get_reply &r) {
  o << "wal_get_reply={ next_offset=" << r.next_offset() << ", gets=("
    << r.gets()->size() << "): [ ";
  for (const auto &g : *r.gets()) {
    o << g;
  }
  o << "]} ";
  return o;
}

ostream &
operator<<(ostream &o, const wal_put_partition_records &r) {
  o << "wal_put_partition_records={partition=" << r.partition() << ", records("
    << r.records()->size() << ")} ";
  return o;
}
ostream &
operator<<(ostream &o, const wal_put_request &r) {
  o << "wal_put_request={ ns=" << r.ns() << ", topic=" << r.topic()
    << ", partition_puts(" << r.partition_puts()->size() << "): [";
  for (const auto &g : *r.partition_puts()) {
    o << g;
  }
  o << "]} ";
  return o;
}
ostream &
operator<<(ostream &o, const wal_put_reply &r) {
  o << "wal_put_reply={ offsets=(" << r.offsets()->size() << "): [";
  for (const auto &g : *r.offsets()) {
    o << g;
  }
  o << "]} ";
  return o;
}

ostream &
operator<<(ostream &o, const wal_read_request &r) {
  o << "wal_read_request={ address: " << r.req << ", req=" << *r.req << "} ";
  return o;
}

ostream &
operator<<(ostream &o, const wal_binary_record &r) {
  o << "wal_binary_record={ data(size): " << r.data()->size() << "} ";
  return o;
}
ostream &
operator<<(ostream &o, const wal_binary_recordT &r) {
  o << "wal_binary_recordT={ data(size): " << r.data.size() << "} ";
  return o;
}

ostream &
operator<<(ostream &o, const wal_read_reply &r) {
  o << "wal_read_reply={ errno:" << EnumNamewal_read_errno(r.reply().error)
    << ", data.next_offset: " << r.reply().next_offset << ", data.gets("
    << r.reply().gets.size() << "): [";
  for (const auto &g : r.reply().gets) {
    o << *g;
  }
  o << "], on_disk_size =" << r.on_disk_size() << " } ";
  return o;
}
ostream &
operator<<(ostream &o, const wal_write_request &r) {
  o << "wal_write_request={ address: " << r.req << ", req=" << *r.req << "}";
  return o;
}
ostream &
operator<<(ostream &o, const wal_write_reply &r) {
  o << "wal_write_reply={ offsets(" << r.size() << "): [";
  if (!r.empty()) {
    for (const auto &g : r) {
      o << *g.second;
    }
  }
  o << "]} ";
  return o;
}

ostream &
operator<<(ostream &o, const wal_put_reply_partition_tuple &t) {
  o << "wal_put_reply_partition_tuple={ partition=" << t.partition()
    << ", start_offset=" << t.start_offset()
    << ", end_offset=" << t.end_offset() << " }";
  return o;
}
ostream &
operator<<(ostream &o, const wal_put_reply_partition_tupleT &t) {
  o << "wal_put_reply_partition_tupleT={ partition=" << t.partition
    << ", start_offset=" << t.start_offset << ", end_offset=" << t.end_offset
    << " } ";
  return o;
}

}  // namespace std
