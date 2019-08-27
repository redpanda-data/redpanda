#include "model/compression.h"
#include "model/fundamental.h"
#include "seastarx.h"

#include <seastar/core/print.hh>

namespace model {

std::ostream& operator<<(std::ostream& os, partition p) {
    return fmt_print(os, "{{partition: {}}}", p.value);
}

std::ostream& operator<<(std::ostream& os, topic_view t) {
    return fmt_print(os, "{{topic: {}}}", t.name());
}

std::ostream& operator<<(std::ostream& os, const topic& t) {
    return fmt_print(os, "{{topic: {}}}", t.name);
}

std::ostream& operator<<(std::ostream& os, const ns& n) {
    return fmt_print(os, "{{namespace: {}}}", n.name);
}

std::ostream& operator<<(std::ostream& os, compression c) {
    os << "{compression: ";
    switch (c) {
    case compression::none:
        os << "none";
        break;
    case compression::gzip:
        os << "gzip";
        break;
    case compression::snappy:
        os << "snappy";
        break;
    case compression::lz4:
        os << "lz4";
        break;
    case compression::zstd:
        os << "zstd";
        break;
    }
    return os << "}";
}

std::ostream& operator<<(std::ostream& os, topic_partition tp) {
    return fmt_print(
      os, "{{topic_partition: {}:{}}}", tp.topic.name, tp.partition);
}

std::ostream& operator<<(std::ostream& os, namespaced_topic_partition ntp) {
    return fmt_print(
      os, "{{namespaced_topic_partition: {}:{}}}", ntp.ns, ntp.tp);
}

std::ostream& operator<<(std::ostream& os, offset o) {
    return fmt_print(os, "{{offset: {}}}", o.value());
}

} // namespace model
