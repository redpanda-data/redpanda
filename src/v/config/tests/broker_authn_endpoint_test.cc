// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/broker_authn_endpoint.h"

#include <seastar/testing/thread_test_case.hh>

#include <yaml-cpp/yaml.h>

#include <optional>
#include <string>
#include <string_view>

namespace {

config::broker_authn_endpoint decode_yaml(std::string_view yaml) {
    auto node = YAML::Load(std::string{yaml});
    return node.as<config::broker_authn_endpoint>();
}

struct parse_case {
    std::string_view name;
    std::string_view yaml;
    bool expect_decode_ok;
    // The ep-level validator is run independently of YAML::decode so we can
    // observe decode-time failures and validator-time failures as distinct
    // signals.
    bool expect_validate_ok;
    std::string_view expected_err_substr;
};

void check(const parse_case& c) {
    BOOST_TEST_CONTEXT("case: " << c.name) {
        config::broker_authn_endpoint ep;
        bool decoded = true;
        try {
            ep = decode_yaml(c.yaml);
        } catch (const YAML::BadConversion&) {
            decoded = false;
        } catch (const YAML::RepresentationException&) {
            decoded = false;
        }
        BOOST_TEST(decoded == c.expect_decode_ok);
        if (!decoded) {
            return;
        }
        auto err = config::validate_broker_authn_endpoint(ep);
        BOOST_TEST(!err.has_value() == c.expect_validate_ok);
        if (err.has_value() && !c.expected_err_substr.empty()) {
            BOOST_TEST(
              std::string_view{*err}.find(c.expected_err_substr)
              != std::string_view::npos);
        }
    }
}

} // namespace

SEASTAR_THREAD_TEST_CASE(inet_only_ok) {
    check(
      {.name = "inet_only_ok",
       .yaml = "name: internal\n"
               "address: 127.0.0.1\n"
               "port: 9092\n",
       .expect_decode_ok = true,
       .expect_validate_ok = true,
       .expected_err_substr = {}});
}

SEASTAR_THREAD_TEST_CASE(uds_only_ok) {
    check(
      {.name = "uds_only_ok",
       .yaml = "name: internal\n"
               "unix_path: /tmp/rp.sock\n",
       .expect_decode_ok = true,
       .expect_validate_ok = true,
       .expected_err_substr = {}});
}

SEASTAR_THREAD_TEST_CASE(uds_with_mode_ok) {
    check(
      {.name = "uds_with_mode_ok",
       .yaml = "name: internal\n"
               "unix_path: /tmp/rp.sock\n"
               "unix_socket_mode: 0600\n",
       .expect_decode_ok = true,
       .expect_validate_ok = true,
       .expected_err_substr = {}});
}

SEASTAR_THREAD_TEST_CASE(neither_inet_nor_uds_decode_fails) {
    check(
      {.name = "neither_inet_nor_uds",
       .yaml = "name: internal\n",
       .expect_decode_ok = false,
       .expect_validate_ok = false,
       .expected_err_substr = {}});
}

SEASTAR_THREAD_TEST_CASE(both_inet_and_uds_decode_fails) {
    check(
      {.name = "both_inet_and_uds",
       .yaml = "name: internal\n"
               "address: 127.0.0.1\n"
               "port: 9092\n"
               "unix_path: /tmp/rp.sock\n",
       .expect_decode_ok = false,
       .expect_validate_ok = false,
       .expected_err_substr = {}});
}

SEASTAR_THREAD_TEST_CASE(uds_relative_path_validator_fails) {
    check(
      {.name = "uds_relative_path",
       .yaml = "name: internal\n"
               "unix_path: rp.sock\n",
       .expect_decode_ok = true,
       .expect_validate_ok = false,
       .expected_err_substr = "absolute"});
}

SEASTAR_THREAD_TEST_CASE(uds_path_too_long_validator_fails) {
    // sun_path is 108 bytes, max configurable is 107.
    std::string long_path = "/" + std::string(107, 'a');
    std::string yaml = "name: internal\nunix_path: " + long_path + "\n";
    check(
      {.name = "uds_path_too_long",
       .yaml = yaml,
       .expect_decode_ok = true,
       .expect_validate_ok = false,
       .expected_err_substr = "too long"});
}

SEASTAR_THREAD_TEST_CASE(uds_path_max_length_ok) {
    // Exactly at the 107-byte limit — should pass.
    std::string at_limit = "/" + std::string(106, 'a');
    std::string yaml = "name: internal\nunix_path: " + at_limit + "\n";
    check(
      {.name = "uds_path_max_length_ok",
       .yaml = yaml,
       .expect_decode_ok = true,
       .expect_validate_ok = true,
       .expected_err_substr = {}});
}

SEASTAR_THREAD_TEST_CASE(uds_empty_path_validator_fails) {
    // Empty-string unix_path passes the decoder's exactly-one-of check (the
    // key is present) but the validator must reject it.
    check(
      {.name = "uds_empty_path",
       .yaml = "name: internal\n"
               "unix_path: \"\"\n",
       .expect_decode_ok = true,
       .expect_validate_ok = false,
       .expected_err_substr = "empty"});
}

SEASTAR_THREAD_TEST_CASE(uds_mode_at_07777_ok) {
    // 07777 = sticky + setuid + setgid + rwxrwxrwx. Upper bound of the
    // range the validator accepts; setgid (02000) in particular is the
    // standard pattern for cross-container bind-mount deployments where
    // the socket should inherit the parent-dir GID.
    check(
      {.name = "uds_mode_at_07777_ok",
       .yaml = "name: internal\n"
               "unix_path: /tmp/rp.sock\n"
               "unix_socket_mode: 4095\n", // 07777
       .expect_decode_ok = true,
       .expect_validate_ok = true,
       .expected_err_substr = {}});
}

SEASTAR_THREAD_TEST_CASE(uds_mode_out_of_range_validator_fails) {
    check(
      {.name = "uds_mode_out_of_range",
       .yaml = "name: internal\n"
               "unix_path: /tmp/rp.sock\n"
               "unix_socket_mode: 4096\n", // 0o10000 — above 07777
       .expect_decode_ok = true,
       .expect_validate_ok = false,
       .expected_err_substr = "out of range"});
}

// -----------------------------------------------------------------------
// Path-sanity security table.
//
// These cases document the exact set of paths the validator accepts and
// rejects. Each entry carries a human-readable `description` that reads
// as a sentence of intent so reviewers (and future us) can scan the
// table without reverse-engineering the assertions.
//
// Threat model in one sentence: a local attacker with write access to
// the parent directory should never be able to trick the broker into
// unlinking an arbitrary file or binding onto an inode they control;
// `prepare_uds_path()` + `verify_uds_bound()` carry the runtime half of
// that job, and this table carries the config-time half.
// -----------------------------------------------------------------------
struct path_case {
    std::string_view name;
    std::string_view description;
    std::string path; // stored out-of-line for dynamic values
    bool expect_validate_ok;
    std::string_view expected_err_substr;
};

static void check_path_case(const path_case& c) {
    BOOST_TEST_CONTEXT("case: " << c.name << " — " << c.description) {
        // Construct the endpoint directly rather than via YAML. The
        // table exercises the validator's path-sanity logic in
        // isolation; YAML round-trip is covered by the separate
        // encode_roundtrip_* cases. Direct construction also side-
        // steps any yaml-cpp quirks around control-character escapes
        // (notably "\0") so the NUL-byte case reliably reaches the
        // validator regardless of the YAML library's handling.
        config::broker_authn_endpoint ep{
          .name = "internal",
          .address = {},
          .authn_method = {},
          .unix_path = ss::sstring(c.path.data(), c.path.size()),
          .unix_socket_mode = {},
        };
        auto err = config::validate_broker_authn_endpoint(ep);
        BOOST_TEST(!err.has_value() == c.expect_validate_ok);
        if (err.has_value() && !c.expected_err_substr.empty()) {
            BOOST_TEST(
              std::string_view{*err}.find(c.expected_err_substr)
                != std::string_view::npos,
              "expected error substring '"
                << c.expected_err_substr << "' not found in '" << *err << "'");
        }
    }
}

SEASTAR_THREAD_TEST_CASE(path_sanity_table) {
    // ------------- Positive cases (must validate_ok = true) -------------
    std::vector<path_case> ok_cases{
      {.name = "typical_var_run",
       .description = "standard /var/run path, the recommended k8s shape",
       .path = "/var/run/redpanda/kafka.sock",
       .expect_validate_ok = true,
       .expected_err_substr = {}},
      {.name = "single_char_leaf",
       .description = "extremely short path — just /a — legal per POSIX",
       .path = "/a",
       .expect_validate_ok = true,
       .expected_err_substr = {}},
      {.name = "at_107_byte_limit",
       .description = "exactly sun_path-1 (107) bytes — at the Linux limit",
       .path = "/" + std::string(106, 'x'),
       .expect_validate_ok = true,
       .expected_err_substr = {}},
      {.name = "deep_nested_path",
       .description = "multiple directory components, no traversal",
       .path = "/a/b/c/d/e/f/g/h.sock",
       .expect_validate_ok = true,
       .expected_err_substr = {}},
      {.name = "dots_inside_name",
       .description = "literal dots inside a basename are fine (foo.bar.sock)",
       .path = "/var/run/redpanda/kafka.api.sock",
       .expect_validate_ok = true,
       .expected_err_substr = {}},
      {.name = "single_dot_not_traversal",
       .description = "lone '.' in a name is not a traversal component",
       .path = "/var/run/redpanda/.hidden.sock",
       .expect_validate_ok = true,
       .expected_err_substr = {}},
    };

    // ------------- Negative cases (must validate_ok = false) -------------
    std::vector<path_case> err_cases{
      {.name = "empty_path",
       .description = "empty string — no inode to create",
       .path = "",
       .expect_validate_ok = false,
       .expected_err_substr = "empty"},
      {.name = "relative_path",
       .description = "no leading slash — relative paths are rejected "
                      "because the effective location depends on CWD at "
                      "bind time, which is not deterministic across "
                      "operator invocations",
       .path = "redpanda.sock",
       .expect_validate_ok = false,
       .expected_err_substr = "absolute"},
      {.name = "relative_dot_slash",
       .description = "./relative is still relative; caught by the "
                      "absolute-path guard",
       .path = "./rp.sock",
       .expect_validate_ok = false,
       .expected_err_substr = "absolute"},
      {.name = "over_107_bytes",
       .description = "108 bytes — one past Linux sun_path limit, would "
                      "be silently truncated by bind(2)",
       .path = "/" + std::string(107, 'x'),
       .expect_validate_ok = false,
       .expected_err_substr = "too long"},
      {.name = "embedded_nul",
       .description = "embedded NUL byte — sun_path truncates here, so "
                      "config and kernel would disagree about the target "
                      "inode. Classic smuggling vector.",
       .path = std::string("/real/path.sock\0/elsewhere", 26),
       .expect_validate_ok = false,
       .expected_err_substr = "NUL"},
      {.name = "parent_traversal_simple",
       .description = "lexical '..' component — the only way an operator "
                      "writes this is either by mistake or to escape a "
                      "restriction. Rejected at parse time so an outer "
                      "admission policy (e.g. 'confine to /var/run/rp') "
                      "is actually enforceable.",
       .path = "/var/run/redpanda/../../etc/passwd",
       .expect_validate_ok = false,
       .expected_err_substr = ".."},
      {.name = "parent_traversal_at_root",
       .description = "traversal attempt originating at root — would "
                      "resolve to /etc/passwd if honored",
       .path = "/../etc/passwd",
       .expect_validate_ok = false,
       .expected_err_substr = ".."},
      {.name = "parent_traversal_trailing",
       .description = "'..' as the final component — a classic way to "
                      "bypass a naive suffix check",
       .path = "/var/run/redpanda/foo/..",
       .expect_validate_ok = false,
       .expected_err_substr = ".."},
      {.name = "double_slash",
       .description = "'//' runs — kernel tolerates them, but they hint "
                      "at a buggy config generator; easier to debug if "
                      "we reject them",
       .path = "/var/run//redpanda/kafka.sock",
       .expect_validate_ok = false,
       .expected_err_substr = "canonical"},
      {.name = "trailing_slash",
       .description = "trailing '/' is never meaningful for a socket "
                      "path — bind(2) treats the path as a filename, "
                      "not a directory",
       .path = "/var/run/redpanda/kafka.sock/",
       .expect_validate_ok = false,
       .expected_err_substr = "trailing"},
    };

    for (const auto& c : ok_cases) {
        check_path_case(c);
    }
    for (const auto& c : err_cases) {
        check_path_case(c);
    }
}

SEASTAR_THREAD_TEST_CASE(uds_with_sasl_auth_ok) {
    config::broker_authn_endpoint ep = decode_yaml(
      "name: internal\n"
      "unix_path: /tmp/rp.sock\n"
      "authentication_method: sasl\n");
    BOOST_TEST(!config::validate_broker_authn_endpoint(ep).has_value());
    BOOST_TEST(ep.is_unix_domain());
    BOOST_REQUIRE(ep.authn_method.has_value());
    BOOST_TEST(
      config::to_string_view(*ep.authn_method)
      == config::to_string_view(config::broker_authn_method::sasl));
}

SEASTAR_THREAD_TEST_CASE(uds_with_mtls_identity_entry_level_ok) {
    // At the entry level, mtls_identity is not rejected; the cross-list
    // validator enforces the TLS-on-UDS constraint (see
    // validate_kafka_uds_constraints).
    config::broker_authn_endpoint ep = decode_yaml(
      "name: internal\n"
      "unix_path: /tmp/rp.sock\n"
      "authentication_method: mtls_identity\n");
    BOOST_TEST(!config::validate_broker_authn_endpoint(ep).has_value());
    BOOST_REQUIRE(ep.authn_method.has_value());
    BOOST_TEST(
      config::to_string_view(*ep.authn_method)
      == config::to_string_view(config::broker_authn_method::mtls_identity));
}

SEASTAR_THREAD_TEST_CASE(encode_roundtrip_uds) {
    config::broker_authn_endpoint src = decode_yaml(
      "name: internal\n"
      "unix_path: /tmp/rp.sock\n"
      "unix_socket_mode: 0660\n"
      "authentication_method: none\n");
    YAML::Node n = YAML::convert<config::broker_authn_endpoint>::encode(src);
    BOOST_TEST(n["unix_path"].as<std::string>() == "/tmp/rp.sock");
    BOOST_TEST(n["unix_socket_mode"].as<uint32_t>() == 0660u);
    BOOST_TEST(!bool(n["address"]));
    BOOST_TEST(!bool(n["port"]));
}

SEASTAR_THREAD_TEST_CASE(encode_roundtrip_inet) {
    config::broker_authn_endpoint src = decode_yaml(
      "name: internal\n"
      "address: 127.0.0.1\n"
      "port: 9092\n");
    YAML::Node n = YAML::convert<config::broker_authn_endpoint>::encode(src);
    BOOST_TEST(n["address"].as<std::string>() == "127.0.0.1");
    BOOST_TEST(n["port"].as<uint16_t>() == 9092);
    BOOST_TEST(!bool(n["unix_path"]));
}

SEASTAR_THREAD_TEST_CASE(duplicate_unix_path_list_validator_fails) {
    std::vector<config::broker_authn_endpoint> eps{
      decode_yaml(
        "name: a\n"
        "unix_path: /tmp/rp.sock\n"),
      decode_yaml(
        "name: b\n"
        "unix_path: /tmp/rp.sock\n"),
    };
    auto err = config::validate_broker_authn_endpoints(eps);
    BOOST_REQUIRE(err.has_value());
    BOOST_TEST(
      std::string_view{*err}.find("duplicate unix_path")
      != std::string_view::npos);
}
