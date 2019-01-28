#pragma once

#include <iostream>

#include <seastar/core/sstring.hh>

#include "wal_generated.h"
#include "wal_requests.h"

namespace std {
ostream &operator<<(ostream &o, const v::wal_get_request &);
ostream &operator<<(ostream &o, const v::wal_get_reply &);
ostream &operator<<(ostream &o, const v::wal_put_request &);
ostream &operator<<(ostream &o, const v::wal_put_reply &);
ostream &operator<<(ostream &o, const v::wal_header &);
ostream &operator<<(ostream &o, const v::wal_put_reply_partition_tupleT &);
ostream &operator<<(ostream &o, const v::wal_put_reply_partition_tuple &);
ostream &operator<<(ostream &o, const v::wal_binary_record &);
ostream &operator<<(ostream &o, const v::wal_binary_recordT &);
ostream &operator<<(ostream &o, const v::wal_put_partition_records &);
ostream &operator<<(ostream &o, const v::wal_read_request &);
ostream &operator<<(ostream &o, const v::wal_read_reply &);
ostream &operator<<(ostream &o, const v::wal_write_request &);
ostream &operator<<(ostream &o, const v::wal_write_reply &);
}  // namespace std
