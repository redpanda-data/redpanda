/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "container/chunked_vector.h"
#include "model/record.h"
#include "pandaproxy/schema_registry/seq_writer.h"
#include "pandaproxy/schema_registry/sharded_store.h"
#include "pandaproxy/schema_registry/storage.h"
#include "pandaproxy/schema_registry/test/utils.h"
#include "pandaproxy/schema_registry/types.h"
#include "storage/record_batch_builder.h"

#include <seastar/core/smp.hh>
#include <seastar/testing/perf_tests.hh>

#include <fmt/format.h>

#include <cstdlib>
#include <string>

namespace pps = pandaproxy::schema_registry;

namespace {

/// Benchmarks Schema Registry startup recovery: replaying an encoded
/// _schemas history through consume_to_store - everything
/// service::ensure_started() does apart from fetching the records.
///
/// The corpus mirrors a production registry: every schema sits atop a
/// deep shared import chain (so a compile pulls in the whole closure),
/// and every record is rewritten `churn` times over (re-registration
/// history that survives in a compacted topic).
///
/// The benchmark deliberately uses only APIs that predate deferred
/// recovery, so it can be run at every commit of the recovery work to
/// measure each change. The deferred pipeline is exercised through the
/// reversed corpus: a record whose references are not yet resolvable is
/// stored raw and marked for the later process_marked_schemas() pass,
/// which is the same store pipeline that deferred recovery drives
/// explicitly for every record.
///
/// - recovery_inline: in-reference-order corpus, replay plus pass. Every
///   record canonicalises (compiles) during the replay; this is recovery
///   with schema_registry_deferred_recovery=false.
/// - marked_replay: reversed corpus, replay only. Records take the
///   raw-and-mark path; an upper bound for the deferred time-to-serving
///   replay (it additionally pays one failed reference resolution per
///   record).
/// - marked_canonicalisation: reversed corpus, pass only. The compile of
///   the store's final state, i.e. deferred recovery's background stage.
///
/// The reported runtime is per replayed record; multiply by a registry's
/// record count to size a recovery. marked_canonicalisation's absolute
/// cost scales with *distinct* schemas rather than history, so at low
/// churn it can read as more expensive per record than the replay. The
/// corpus is scalable via environment variables to reproduce
/// production-sized totals, e.g.:
///   RECOVERY_BENCH_SUBJECTS=7500 RECOVERY_BENCH_CHURN=60 \
///     bazel run --config=release \
///     //src/v/pandaproxy/schema_registry/test:recovery_rpbench -- \
///     -t '.*marked.*'
/// (recovery_inline at that scale takes minutes per iteration.)
constexpr size_t schema_bytes = 3072;
constexpr size_t records_per_batch = 100;

int env_or(const char* name, int fallback) {
    if (const char* v = std::getenv(name); v != nullptr) {
        return std::stoi(v);
    }
    return fallback;
}

const int chain_depth = env_or("RECOVERY_BENCH_CHAIN", 30);
const int n_subjects = env_or("RECOVERY_BENCH_SUBJECTS", 100);
const int churn = env_or("RECOVERY_BENCH_CHURN", 10);

std::string pad_fields(std::string header, std::string_view tail) {
    int field_idx = 1;
    while (header.size() < schema_bytes) {
        header += fmt::format(
          "  // padding comment for field number {:08}\n"
          "  string field_{:08} = {};\n",
          field_idx,
          field_idx,
          field_idx);
        ++field_idx;
    }
    header += tail;
    return header;
}

std::string make_common_proto(int k) {
    auto header = fmt::format(
      "syntax = \"proto3\";\npackage bench.common{};\n", k);
    std::string embed;
    if (k > 0) {
        header += fmt::format("import \"common_{}.proto\";\n", k - 1);
        embed = fmt::format(
          "  bench.common{}.Rec{} prev = 15000;\n}}\n", k - 1, k - 1);
    } else {
        embed = "}\n";
    }
    header += fmt::format("message Rec{} {{\n", k);
    return pad_fields(std::move(header), embed);
}

std::string make_subject_proto(int i) {
    const int tail = chain_depth - 1;
    auto header = fmt::format(
      "syntax = \"proto3\";\n"
      "package bench.subject{};\n"
      "import \"common_{}.proto\";\n"
      "message Payload{} {{\n"
      "  bench.common{}.Rec{} rec = 15000;\n",
      i,
      tail,
      i,
      tail,
      tail);
    return pad_fields(std::move(header), "}\n");
}

pps::schema_definition::references make_refs(const ss::sstring& target) {
    pps::schema_definition::references refs;
    refs.push_back(
      pps::schema_reference{
        .name = target,
        .sub = pps::context_subject_reference::unqualified(target),
        .version = pps::schema_version{1}});
    return refs;
}

struct encoded_corpus {
    chunked_vector<model::record_batch> batches;
    size_t records{0};
};

/// With in_reference_order, references precede their referents (the
/// import chain, then every subject's record repeated `churn` times), so
/// every record can canonicalise as it is applied. Reversed, no record's
/// references are resolvable at apply time, so every record is stored
/// raw and marked, leaving all compilation to
/// process_marked_schemas() - the deferred recovery pipeline.
///
/// Keys are unsequenced, as consume_to_store receives for
/// third-party/imported records, so they are applied unconditionally.
encoded_corpus encode_corpus(bool in_reference_order) {
    encoded_corpus corpus;
    const auto ver1 = pps::schema_version{1};

    std::unique_ptr<storage::record_batch_builder> rb;
    auto flush = [&corpus, &rb]() {
        if (rb) {
            corpus.batches.push_back(std::move(*rb).build());
            rb.reset();
        }
    };
    auto add = [&](
                 pps::context_subject sub,
                 pps::schema_id id,
                 pps::schema_definition def) {
        if (!rb) {
            rb = std::make_unique<storage::record_batch_builder>(
              model::record_batch_type::raft_data,
              model::offset(corpus.records));
        }
        // Serialize the key before the value moves `sub` out; the two
        // arguments of add_raw_kv are unsequenced.
        auto key = to_json_iobuf(
          pps::schema_key{
            .seq{std::nullopt},
            .node{std::nullopt},
            .sub{sub},
            .version{ver1}});
        rb->add_raw_kv(
          std::move(key),
          to_json_iobuf(
            pps::schema_value{
              .schema{std::move(sub), std::move(def)},
              .version{ver1},
              .id{id}}));
        if (++corpus.records % records_per_batch == 0) {
            flush();
        }
    };
    auto add_common = [&](int k) {
        auto name = fmt::format("common_{}.proto", k);
        pps::schema_definition::references refs;
        if (k > 0) {
            refs = make_refs(fmt::format("common_{}.proto", k - 1));
        }
        add(
          pps::context_subject::unqualified(name),
          pps::schema_id{k + 1},
          pps::schema_definition{
            iobuf::from(make_common_proto(k)),
            pps::schema_type::protobuf,
            std::move(refs),
            std::nullopt});
    };
    auto add_subject =
      [&](int i, const pps::schema_definition::references& refs) {
          auto sub = pps::context_subject::unqualified(
            fmt::format("bench/subject{}.proto", i));
          auto def = pps::schema_definition{
            iobuf::from(make_subject_proto(i)),
            pps::schema_type::protobuf,
            refs.copy(),
            std::nullopt};
          for (int c = 0; c < churn; ++c) {
              add(sub, pps::schema_id{chain_depth + i + 1}, def.share());
          }
      };

    auto tail_refs = make_refs(fmt::format("common_{}.proto", chain_depth - 1));
    if (in_reference_order) {
        for (int k = 0; k < chain_depth; ++k) {
            add_common(k);
        }
        for (int i = 0; i < n_subjects; ++i) {
            add_subject(i, tail_refs);
        }
    } else {
        for (int i = 0; i < n_subjects; ++i) {
            add_subject(i, tail_refs);
        }
        for (int k = chain_depth - 1; k >= 0; --k) {
            add_common(k);
        }
    }
    flush();
    return corpus;
}

class recovery_bench {
public:
    /// Replay an encoded corpus through consume_to_store into a fresh
    /// store, as service recovery does after fetching. Measures the
    /// replay, and the process_marked_schemas() pass if requested;
    /// returns the record count so results read as time per record.
    ss::future<size_t>
    replay(bool in_reference_order, bool measure_replay, bool canonicalise) {
        auto& corpus = in_reference_order ? _in_order : _reversed;
        if (corpus.batches.empty()) {
            corpus = encode_corpus(in_reference_order);
        }

        pps::sharded_store store;
        co_await store.start(
          pps::is_mutable::yes, ss::default_smp_service_group());
        noop_transport transport;
        ss::sharded<pps::seq_writer> seq;
        co_await seq.start(
          model::node_id{0},
          ss::default_smp_service_group(),
          std::ref(transport),
          std::reference_wrapper(store),
          ss::sharded_parameter(
            [] { return std::make_unique<sequence_state_checker_test>(); }));

        auto consumer = pps::consume_to_store{store, seq.local()};
        if (measure_replay) {
            perf_tests::start_measuring_time();
        }
        for (const auto& batch : corpus.batches) {
            co_await consumer(batch.copy());
        }
        if (!measure_replay) {
            perf_tests::start_measuring_time();
        }
        if (canonicalise) {
            co_await store.process_marked_schemas();
        }
        perf_tests::stop_measuring_time();

        co_await seq.stop();
        co_await store.stop();
        co_return corpus.records;
    }

private:
    encoded_corpus _in_order;
    encoded_corpus _reversed;
};

} // namespace

// Recovery where every record canonicalises (compiles) as it is
// replayed: schema_registry_deferred_recovery=false.
PERF_TEST_CN(recovery_bench, recovery_inline) {
    co_return co_await replay(true, true, true);
}

// A replay in which every record takes the raw-and-mark path: an upper
// bound for deferred recovery's time-to-serving replay stage.
PERF_TEST_CN(recovery_bench, marked_replay) {
    co_return co_await replay(false, true, false);
}

// The process_marked_schemas() pass over a fully-marked store: deferred
// recovery's background canonicalisation stage. Its cost scales with
// distinct schemas rather than replayed records.
PERF_TEST_CN(recovery_bench, marked_canonicalisation) {
    co_return co_await replay(false, false, true);
}
