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

} // namespace model
