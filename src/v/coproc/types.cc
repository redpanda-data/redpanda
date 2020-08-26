#include "types.h"

#include "model/validation.h"

namespace coproc {

std::optional<materialized_topic>
make_materialized_topic(const model::topic& topic) {
    // Materialized topics will follow this schema:
    // <source_topic>.$<destination_topic>$
    // Note that dollar signs are not valid kafka topic names but they
    // can be part of a valid coproc materialized_topic name
    const auto found = topic().find('.');
    if (
      found == ss::sstring::npos || found == 0 || found == topic().size() - 1) {
        // If the char '.' is never found, or at a position that would make
        // either of the parts empty, fail out
        return std::nullopt;
    }
    std::string_view src(topic().begin(), found);
    std::string_view dest(
      (topic().begin() + found + 1), topic().size() - found - 1);
    if (!(dest.size() >= 3 && dest[0] == '$' && dest.back() == '$')) {
        // Dest must have at least two dollar chars surronding the topic name
        return std::nullopt;
    }

    // No need for '$' chars in the string_view, its implied by being in the
    // 'dest' mvar
    dest.remove_prefix(1);
    dest.remove_suffix(1);

    model::topic_view src_tv(src);
    model::topic_view dest_tv(dest);
    if (
      model::validate_kafka_topic_name(src_tv).value() != 0
      || model::validate_kafka_topic_name(dest_tv).value() != 0) {
        // The parts of this whole must be valid kafka topics
        return std::nullopt;
    }

    return materialized_topic{.src = src_tv, .dest = dest_tv};
}

} // namespace coproc
