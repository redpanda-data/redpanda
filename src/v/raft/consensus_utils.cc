#include "raft/consensus_utils.h"

#include <yaml-cpp/yaml.h>

namespace YAML {
template<>
struct convert<::raft::consensus::voted_for_configuration> {
    static Node encode(const ::raft::consensus::voted_for_configuration& c) {
        Node node;
        node["voted_for"] = c.voted_for();
        node["term"] = c.term();
        return node;
    }
    static bool
    decode(const Node& node, ::raft::consensus::voted_for_configuration& c) {
        for (auto ref : {"term", "voted_for"}) {
            if (!node[ref]) {
                return false;
            }
        }
        c.voted_for = model::node_id(
          node["voted_for"].as<model::node_id::type>());
        c.term = model::term_id(node["term"].as<model::term_id::type>());
        return true;
    }
};
} // namespace YAML

namespace raft::details {
future<temporary_buffer<char>> readfile(sstring name) {
    return open_file_dma(std::move(name), open_flags::ro | open_flags::create)
      .then([](file f) {
          return f.size()
            .then([f](uint64_t size) mutable {
                return f.dma_read_bulk<char>(0, size);
            })
            .then([f](temporary_buffer<char> b) mutable {
                return f.close().then(
                  [f, b = std::move(b)]() mutable { return std::move(b); });
            });
      });
}

future<> writefile(sstring name, temporary_buffer<char> buf) {
    auto flags = open_flags::wo | open_flags::create | open_flags::truncate;
    return open_file_dma(std::move(name), flags)
      .then([b = std::move(buf)](file f) {
          auto out = make_lw_shared<output_stream<char>>(
            make_file_output_stream(std::move(f)));
          return out->write(b.get(), b.size())
            .then([out] { return out->flush(); })
            .then([out] { return out->close(); })
            .finally([out] {});
      });
}
future<>
persist_voted_for(sstring filename, consensus::voted_for_configuration r) {
    YAML::Emitter out;
    out << YAML::convert<consensus::voted_for_configuration>::encode(r);
    temporary_buffer<char> buf(out.size());
    std::copy_n(out.c_str(), out.size(), buf.get_write());
    return writefile(filename, std::move(buf));
}

future<consensus::voted_for_configuration> read_voted_for(sstring filename) {
    return readfile(filename).then([](temporary_buffer<char> buf) {
        if (buf.empty()) {
            return consensus::voted_for_configuration{};
        }
        // unfort it uses a std::string override
        // XXX update when YAML::Load supports string_view()'s
        // see https://github.com/jbeder/yaml-cpp/issues/765
        std::string s(buf.get(), buf.size());
        auto node = YAML::Load(s);
        return node.as<consensus::voted_for_configuration>();
    });
}

} // namespace raft::details
