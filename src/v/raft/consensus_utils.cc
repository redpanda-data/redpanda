#include "raft/consensus_utils.h"

#include "likely.h"
#include "model/record.h"
#include "random/generators.h"
#include "resource_mgmt/io_priority.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/seastar.hh>
#include <seastar/core/thread.hh>

#include <filesystem>
// delete
#include <seastar/core/future-util.hh>

#include <yaml-cpp/yaml.h>

#include <algorithm>
#include <iterator>

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
ss::future<ss::temporary_buffer<char>> readfile(ss::sstring name) {
    return ss::open_file_dma(
             std::move(name), ss::open_flags::ro | ss::open_flags::create)
      .then([](ss::file f) {
          return f.size()
            .then([f](uint64_t size) mutable {
                return f.dma_read_bulk<char>(0, size);
            })
            .then([f](ss::temporary_buffer<char> b) mutable {
                return f.close().then(
                  [f, b = std::move(b)]() mutable { return std::move(b); });
            });
      });
}

ss::future<> writefile(ss::sstring name, ss::temporary_buffer<char> buf) {
    auto flags = ss::open_flags::wo | ss::open_flags::create
                 | ss::open_flags::truncate;
    auto tmp_name = fmt::format(
      "{}.atomic.{}", name, random_generators::gen_alphanum_string(8));
    return ss::open_file_dma(tmp_name, flags)
      .then([b = std::move(buf)](ss::file f) {
          auto out = ss::make_lw_shared<ss::output_stream<char>>(
            make_file_output_stream(std::move(f)));
          return out->write(b.get(), b.size())
            .then([out] { return out->flush(); })
            .then([out] { return out->close(); })
            .finally([out] {});
      })
      .then([tmp_name, name] {
          return ss::rename_file(tmp_name, name).then([tmp_name] {
              return ss::remove_file(tmp_name);
          });
      })
      .then([path = std::filesystem::path(name.c_str())] {
          return ss::sync_directory(path.parent_path().string());
      });
}
ss::future<>
persist_voted_for(ss::sstring filename, consensus::voted_for_configuration r) {
    YAML::Emitter out;
    out << YAML::convert<consensus::voted_for_configuration>::encode(r);
    ss::temporary_buffer<char> buf(out.size());
    std::copy_n(out.c_str(), out.size(), buf.get_write());
    return writefile(filename, std::move(buf));
}

ss::future<consensus::voted_for_configuration>
read_voted_for(ss::sstring filename) {
    return readfile(filename).then([](ss::temporary_buffer<char> buf) {
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

static inline void check_copy_out_of_range(size_t expected, size_t got) {
    if (unlikely(expected != got)) {
        throw std::out_of_range("consensus_utils copy out of bounds");
    }
}

static inline ss::circular_buffer<model::record_batch> share_n_record_batch(
  model::record_batch batch,
  const size_t copies,
  const bool use_foreign_iobuf_share) {
    using ret_t = ss::circular_buffer<model::record_batch>;
    ret_t ret;
    ret.reserve(copies);
    // the fast path
    if (!use_foreign_iobuf_share) {
        std::generate_n(
          std::back_inserter(ret),
          copies,
          [batch = std::move(batch)]() mutable { return batch.share(); });
        return ret;
    }

    // foreign share
    std::generate_n(
      std::back_inserter(ret), copies, [batch = std::move(batch)]() mutable {
          return batch.foreign_share();
      });
    return ret;
}

static inline ss::future<std::vector<ss::circular_buffer<model::record_batch>>>
share_n_batches(
  ss::circular_buffer<model::record_batch> batches,
  const size_t copies,
  const bool use_foreign_iobuf_share) {
    using ret_t = std::vector<ss::circular_buffer<model::record_batch>>;
    return do_with(
      std::move(batches),
      ret_t(copies),
      [copies, use_foreign_iobuf_share](
        ss::circular_buffer<model::record_batch>& batches, ret_t& data) {
          return ss::do_for_each(
                   batches,
                   [copies, use_foreign_iobuf_share, &data](
                     model::record_batch& b) mutable {
                       auto shared_batches = share_n_record_batch(
                         std::move(b), copies, use_foreign_iobuf_share);

                       for (auto& buf : data) {
                           buf.push_back(std::move(shared_batches.back()));
                           shared_batches.pop_back();
                       }
                   })
            .then([&data]() mutable { return std::move(data); });
      });
} // namespace raft::details

ss::future<std::vector<model::record_batch_reader>> share_reader(
  model::record_batch_reader rdr,
  const size_t ncopies,
  const bool use_foreign_iobuf_share) {
    return model::consume_reader_to_memory(std::move(rdr), model::no_timeout)
      .then([ncopies, use_foreign_iobuf_share](
              ss::circular_buffer<model::record_batch> batches) {
          return share_n_batches(
            std::move(batches), ncopies, use_foreign_iobuf_share);
      })
      .then([ncopies, use_foreign_iobuf_share](
              std::vector<ss::circular_buffer<model::record_batch>> batches) {
          check_copy_out_of_range(ncopies, batches.size());
          std::vector<model::record_batch_reader> retval;
          retval.reserve(ncopies);
          for (auto& b : batches) {
              retval.push_back(
                model::make_memory_record_batch_reader(std::move(b)));
          }
          check_copy_out_of_range(ncopies, retval.size());
          return retval;
      });
}

ss::future<std::vector<model::record_batch_reader>>
foreign_share_n(model::record_batch_reader&& r, std::size_t ncopies) {
    return share_reader(std::move(r), ncopies, true);
}

ss::future<std::vector<model::record_batch_reader>>
share_n(model::record_batch_reader&& r, std::size_t ncopies) {
    return share_reader(std::move(r), ncopies, false);
}

ss::future<configuration_bootstrap_state>
read_bootstrap_state(storage::log log) {
    // TODO(agallego, michal) - iterate the log in reverse
    // as an optimization
    auto rcfg = storage::log_reader_config(
      log.start_offset(), log.max_offset(), raft_priority());
    auto cfg_state = std::make_unique<configuration_bootstrap_state>();
    return log.make_reader(rcfg).then(
      [state = std::move(cfg_state)](
        model::record_batch_reader reader) mutable {
          auto raw = state.get();
          return std::move(reader)
            .consume(
              do_for_each_batch_consumer([raw](model::record_batch batch) {
                  raw->process_batch(std::move(batch));
                  return ss::make_ready_future<>();
              }),
              model::no_timeout)
            .then([s = std::move(state)]() mutable { return std::move(*s); });
      });
}

ss::future<std::optional<raft::group_configuration>>
extract_configuration(model::record_batch_reader&& reader) {
    using cfg_t = std::optional<raft::group_configuration>;
    return ss::do_with(
      std::move(reader),
      cfg_t{},
      [](model::record_batch_reader& reader, cfg_t& cfg) {
          return reader
            .consume(
              do_for_each_batch_consumer([&cfg](model::record_batch b) mutable {
                  // reader may contain different batches, skip the ones that
                  // does not have configuration
                  if (b.header().type != raft::configuration_batch_type) {
                      return ss::make_ready_future<>();
                  }
                  if (b.compressed()) {
                      return ss::make_exception_future(std::runtime_error(
                        "Compressed configuration records are "
                        "unsupported"));
                  }
                  auto it = std::prev(b.end());
                  cfg = reflection::adl<raft::group_configuration>{}.from(
                    it->share_value());
                  return ss::make_ready_future<>();
              }),
              model::no_timeout)
            .then([&cfg]() mutable { return std::move(cfg); });
      });
}
ss::circular_buffer<model::record_batch>
serialize_configuration_as_batches(group_configuration cfg) {
    auto batch = std::move(
                   storage::record_batch_builder(
                     raft::configuration_batch_type, model::offset(0))
                     .add_raw_kv(iobuf(), reflection::to_iobuf(std::move(cfg))))
                   .build();
    ss::circular_buffer<model::record_batch> batches;
    batches.reserve(1);
    batches.push_back(std::move(batch));
    return batches;
}

model::record_batch_reader serialize_configuration(group_configuration cfg) {
    return model::make_memory_record_batch_reader(
      serialize_configuration_as_batches(std::move(cfg)));
}

} // namespace raft::details
