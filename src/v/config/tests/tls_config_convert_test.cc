// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "config/configuration.h"
#include "config/tls_config.h"
#include "utils/to_string.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <yaml-cpp/exceptions.h>
#include <yaml-cpp/yaml.h>

config::tls_config read_from_yaml(ss::sstring yaml_string) {
    auto node = YAML::Load(yaml_string);
    return node["tls_config"].as<config::tls_config>();
}

SEASTAR_THREAD_TEST_CASE(test_decode_empty) {
    auto empty = "tls_config:\n";
    auto empty_cfg = read_from_yaml(empty);

    BOOST_TEST(!empty_cfg.is_enabled());
    BOOST_TEST(!empty_cfg.get_key_cert_files());
    BOOST_TEST(!empty_cfg.get_truststore_file());
    BOOST_TEST(!empty_cfg.get_require_client_auth());
}

SEASTAR_THREAD_TEST_CASE(test_decode_full_abs_path) {
    auto with_values = "tls_config:\n"
                       "  enabled: true\n"
                       "  cert_file: /fake/cret_file.crt\n"
                       "  key_file: /fake/key_file.key\n"
                       "  truststore_file: /fake/truststore\n"
                       "  crl_file: /fake/crl\n"
                       "  require_client_auth: true\n";
    auto full_cfg = read_from_yaml(with_values);
    BOOST_TEST(full_cfg.is_enabled());
    const auto& key_cert = std::get<config::key_cert>(
      *full_cfg.get_key_cert_files());
    BOOST_TEST(key_cert.key_file == "/fake/key_file.key");
    BOOST_TEST(key_cert.cert_file == "/fake/cret_file.crt");
    BOOST_TEST(*full_cfg.get_truststore_file() == "/fake/truststore");
    BOOST_TEST(*full_cfg.get_crl_file() == "/fake/crl");
    BOOST_TEST(full_cfg.get_require_client_auth());
}

SEASTAR_THREAD_TEST_CASE(test_decode_full_rel_path) {
    auto with_values = "tls_config:\n"
                       "  enabled: true\n"
                       "  cert_file: ./cret_file.crt\n"
                       "  key_file: ./key_file.key\n"
                       "  truststore_file: ./truststore\n"
                       "  crl_file: ./crl\n"
                       "  require_client_auth: true\n";
    auto full_cfg = read_from_yaml(with_values);
    BOOST_TEST(full_cfg.is_enabled());
    const auto& key_cert = std::get<config::key_cert>(
      *full_cfg.get_key_cert_files());
    BOOST_TEST(key_cert.key_file != "./key_file.key");
    BOOST_TEST(key_cert.cert_file != "./cret_file.crt");
    BOOST_TEST(*full_cfg.get_truststore_file() != "./truststore");
    BOOST_TEST(*full_cfg.get_crl_file() != "./crl");
    BOOST_TEST(full_cfg.get_require_client_auth());
}

SEASTAR_THREAD_TEST_CASE(test_decode_default_config) {
    auto with_values = "tls_config:\n"
                       "  enabled: false\n"
                       "  cert_file: \"\"\n"
                       "  key_file: \"\"\n"
                       "  truststore_file: \"\"\n"
                       "  crl_file: \"\"\n"
                       "  require_client_auth: false\n";
    auto empty_cfg = read_from_yaml(with_values);
    BOOST_TEST(!empty_cfg.is_enabled());
    BOOST_TEST(!empty_cfg.get_key_cert_files());
    BOOST_TEST(!empty_cfg.get_truststore_file());
    BOOST_TEST(!empty_cfg.get_crl_file());
    BOOST_TEST(!empty_cfg.get_require_client_auth());
}

SEASTAR_THREAD_TEST_CASE(test_decode_enabled_but_contains_empty_path) {
    auto with_values = "tls_config:\n"
                       "  enabled: true\n"
                       "  cert_file: \"\"\n"
                       "  key_file: \"\"\n"
                       "  truststore_file: \"\"\n"
                       "  crl_file: \"\"\n"
                       "  require_client_auth: false\n";
    auto full_cfg = read_from_yaml(with_values);
    BOOST_TEST(full_cfg.is_enabled());
    const auto& key_cert = std::get<config::key_cert>(
      *full_cfg.get_key_cert_files());
    BOOST_TEST(key_cert.key_file == "");
    BOOST_TEST(key_cert.cert_file == "");
    BOOST_TEST(*full_cfg.get_truststore_file() == "");
    BOOST_TEST(*full_cfg.get_crl_file() == "");
    BOOST_TEST(!full_cfg.get_require_client_auth());
}

SEASTAR_THREAD_TEST_CASE(test_decode_p12_file) {
    auto with_values = "tls_config:\n"
                       "  enabled: true\n"
                       "  truststore_file: /fake/truststore\n"
                       "  crl_file: /fake/crl\n"
                       "  require_client_auth: true\n"
                       "  p12_file: /fake/temp.pfx\n"
                       "  p12_password: test\n";
    auto full_cfg = read_from_yaml(with_values);
    BOOST_TEST(full_cfg.is_enabled());
    const auto& p12_bag = std::get<config::p12_container>(
      *full_cfg.get_key_cert_files());
    BOOST_TEST(p12_bag.p12_path == "/fake/temp.pfx");
    BOOST_TEST(p12_bag.p12_password == "test");
    BOOST_TEST(*full_cfg.get_truststore_file() == "/fake/truststore");
    BOOST_TEST(*full_cfg.get_crl_file() == "/fake/crl");
    BOOST_TEST(full_cfg.get_require_client_auth());
}

SEASTAR_THREAD_TEST_CASE(test_decode_p12_full_rel_path) {
    auto with_values = "tls_config:\n"
                       "  enabled: true\n"
                       "  truststore_file: ./truststore\n"
                       "  crl_file: ./crl\n"
                       "  require_client_auth: true\n"
                       "  p12_file: ./temp.pfx\n"
                       "  p12_password: test\n";
    auto full_cfg = read_from_yaml(with_values);
    BOOST_TEST(full_cfg.is_enabled());
    const auto& p12_bag = std::get<config::p12_container>(
      *full_cfg.get_key_cert_files());
    BOOST_TEST(p12_bag.p12_path != "./temp.pfx");
    BOOST_TEST(p12_bag.p12_password == "test");
    BOOST_TEST(*full_cfg.get_truststore_file() != "./truststore");
    BOOST_TEST(*full_cfg.get_crl_file() != "./crl");
    BOOST_TEST(full_cfg.get_require_client_auth());
}

SEASTAR_THREAD_TEST_CASE(test_decode_p12_and_key_cert) {
    auto with_values = "tls_config:\n"
                       "  enabled: true\n"
                       "  cert_file: /fake/cret_file.crt\n"
                       "  key_file: /fake/key_file.key\n"
                       "  truststore_file: /fake/truststore\n"
                       "  crl_file: /fake/crl\n"
                       "  require_client_auth: true\n"
                       "  p12_file: /fake/temp.pfx\n"
                       "  p12_password: test\n";
    try {
        read_from_yaml(with_values);
        BOOST_REQUIRE(false);
    } catch (const YAML::TypedBadConversion<config::tls_config>&) {
        // good!
    }
}

SEASTAR_THREAD_TEST_CASE(test_decode_p12_missing_password) {
    auto with_values = "tls_config:\n"
                       "  enabled: true\n"
                       "  truststore_file: /fake/truststore\n"
                       "  crl_file: /fake/crl\n"
                       "  require_client_auth: true\n"
                       "  p12_file: /fake/temp.pfx\n";
    try {
        read_from_yaml(with_values);
        BOOST_REQUIRE(false);
    } catch (const YAML::TypedBadConversion<config::tls_config>&) {
        // good!
    }
}
