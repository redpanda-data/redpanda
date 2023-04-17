/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "storage/file_sanitizer_types.h"
#include "test_utils/tmp_dir.h"

#include <seastar/core/fstream.hh>
#include <seastar/core/seastar.hh>
#include <seastar/testing/thread_test_case.hh>

const ss::sstring valid_schema = R"(
{
  "seed": 0,
  "ntps": [
    {
      "namespace": "kafka",
      "topic": "test",
      "partition": 1,
      "failure_configs": {
        "write": {
          "batch_type": "batch_type::raft_data",
          "failure_probability": 0.1,
          "delay_probability": 100,
          "min_delay_ms": 0,
          "max_delay_ms": 100
        }
      }
    }
  ]
}
)";

const ss::sstring missing_seed_schema = R"(
{
  "ntps": [
    {
      "namespace": "kafka",
      "topic": "test",
      "partition": 1,
      "failure_configs": {
        "write": {
          "batch_type": "batch_type::raft_data",
          "failure_probability": 100,
          "delay_probability": 100,
          "min_delay_ms": 0,
          "max_delay_ms": 100
        }
      }
    }
  ]
}
)";

const ss::sstring invalid_op_schema = R"(
{
  "ntps": [
    {
      "namespace": "kafka",
      "topic": "test",
      "partition": 1,
      "failure_configs": {
        "write_and_flush": {
          "batch_type": "batch_type::raft_data",
          "failure_probability": 100,
          "delay_probability": 100,
          "min_delay_ms": 0,
          "max_delay_ms": 100
        }
      }
    }
  ]
}
)";

const ss::sstring invalid_probs_schema = R"(
{
  "ntps": [
    {
      "namespace": "kafka",
      "topic": "test",
      "partition": 1,
      "failure_configs": {
        "write_and_flush": {
          "batch_type": "batch_type::raft_data",
          "failure_probability": -1,
          "delay_probability": 101,
          "min_delay_ms": 0,
          "max_delay_ms": 100
        }
      }
    }
  ]
}
)";

const ss::sstring invalid_batch_schema = R"(
{
  "ntps": [
    {
      "namespace": "kafka",
      "topic": "test",
      "partition": 1,
      "failure_configs": {
        "write_and_flush": {
          "batch_type": "batch_type::not_a_batch_type",
          "failure_probability": 0,
          "delay_probability": 100,
          "min_delay_ms": 0,
          "max_delay_ms": 100
        }
      }
    }
  ]
}
)";

void write_to_file(ss::file& f, const ss::sstring& cfg) {
    iobuf buf;
    buf.append(cfg.data(), cfg.length());

    auto input = make_iobuf_input_stream(std::move(buf));
    auto out = ss::make_file_output_stream(f).get();

    ss::copy(input, out).get();
    out.flush().get();
    out.close().get();
}

SEASTAR_THREAD_TEST_CASE(file_sanitizer_config_parse_test) {
    temporary_dir tmp_dir("file_sanitizer_test");
    auto path = tmp_dir.get_path();

    auto valid_path = path / "valid.json";

    auto missing_seed_path = path / "missing_seed.json";
    auto invalid_op_path = path / "invalid_op.json";
    auto invalid_probs_path = path / "invalid_probs.json";
    auto invalid_batch_path = path / "invalid_batch.json";
    auto missing_path = path / "missing.json";

    auto flags = ss::open_flags::rw | ss::open_flags::create
                 | ss::open_flags::exclusive;

    for (const auto& [path, schema] :
         {std::make_pair(valid_path, valid_schema),
          std::make_pair(missing_seed_path, missing_seed_schema),
          std::make_pair(invalid_op_path, invalid_op_schema),
          std::make_pair(invalid_probs_path, invalid_probs_schema),
          std::make_pair(invalid_batch_path, invalid_batch_schema)}) {
        auto f = ss::open_file_dma(path.native(), flags).get();
        write_to_file(f, schema);
    }

    // Attempt to create a config from files with incorrect or missing
    // schemas.
    for (const auto& path :
         {missing_seed_path,
          invalid_op_path,
          invalid_probs_path,
          invalid_batch_path,
          missing_path}) {
        auto res = storage::make_finjector_file_config(path).get();
        BOOST_REQUIRE(!res.has_value());
    }

    // Create a configuration from the valid input and validate its contents.
    auto cfg = storage::make_finjector_file_config(valid_path).get();
    BOOST_REQUIRE(cfg.has_value());

    model::ntp ntp{"kafka", "test", 1};
    const auto& ntp_cfg = cfg->get_config_for_ntp(ntp);
    BOOST_REQUIRE(ntp_cfg.has_value());

    BOOST_REQUIRE_EQUAL(ntp_cfg->sanitize_only, false);
    BOOST_REQUIRE_EQUAL(ntp_cfg->finjection_cfg.has_value(), true);

    const auto& finject_cfg = ntp_cfg->finjection_cfg.value();

    BOOST_REQUIRE_EQUAL(finject_cfg.ntp, ntp);
    BOOST_REQUIRE_EQUAL(finject_cfg.op_configs.size(), 1);

    const auto& write_failure_cfg = finject_cfg.op_configs[0];
    BOOST_REQUIRE_EQUAL(
      write_failure_cfg.op_type, storage::failable_op_type::write);
    BOOST_REQUIRE_EQUAL(
      write_failure_cfg.batch_type.value(),
      model::record_batch_type::raft_data);
    BOOST_REQUIRE_EQUAL(write_failure_cfg.failure_probability.value(), 0.1);
    BOOST_REQUIRE_EQUAL(write_failure_cfg.delay_probability.value(), 100);
    BOOST_REQUIRE_EQUAL(write_failure_cfg.min_delay_ms.value(), 0);
    BOOST_REQUIRE_EQUAL(write_failure_cfg.max_delay_ms.value(), 100);
}
