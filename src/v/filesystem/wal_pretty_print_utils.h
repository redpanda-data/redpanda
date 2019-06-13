#pragma once

#include "filesystem/wal_generated.h"
#include "filesystem/wal_requests.h"

#include <seastar/core/sstring.hh>

#include <iostream>

namespace std {
ostream& operator<<(ostream& o, const wal_get_request&);
ostream& operator<<(ostream& o, const wal_get_reply&);
ostream& operator<<(ostream& o, const wal_put_request&);
ostream& operator<<(ostream& o, const wal_put_reply&);
ostream& operator<<(ostream& o, const wal_header&);
ostream& operator<<(ostream& o, const wal_put_reply_partition_tupleT&);
ostream& operator<<(ostream& o, const wal_put_reply_partition_tuple&);
ostream& operator<<(ostream& o, const wal_binary_record&);
ostream& operator<<(ostream& o, const wal_binary_recordT&);
ostream& operator<<(ostream& o, const wal_put_partition_records&);
ostream& operator<<(ostream& o, const wal_read_request&);
ostream& operator<<(ostream& o, const wal_read_reply&);
ostream& operator<<(ostream& o, const wal_write_request&);
ostream& operator<<(ostream& o, const wal_write_reply&);
} // namespace std
