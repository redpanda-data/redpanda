#include "config/configuration.h"
#include "utils/to_string.h"

#include <seastar/core/thread.hh>
#include <seastar/testing/thread_test_case.hh>

#include <yaml-cpp/yaml.h>

auto empty = "tls_config:\n";
auto with_values = "tls_config:\n"
                   "  enabled: true\n"
                   "  cert_file: /fake/cret_file.crt\n"
                   "  key_file: /fake/key_file.key\n"
                   "  truststore_file: /fake/truststore\n"
                   "  require_client_auth: true\n";

config::tls_config read_from_yaml(sstring yaml_string) {
    auto node = YAML::Load(yaml_string);
    return node["tls_config"].as<config::tls_config>();
}

SEASTAR_THREAD_TEST_CASE(test_decode) {
    auto empty_cfg = read_from_yaml(empty);

    BOOST_TEST(!empty_cfg.is_enabled());
    BOOST_TEST(!empty_cfg.get_key_cert_files());
    BOOST_TEST(!empty_cfg.get_truststore_file());
    BOOST_TEST(!empty_cfg.get_require_client_auth());

    auto full_cfg = read_from_yaml(with_values);
    BOOST_TEST(full_cfg.is_enabled());
    BOOST_TEST(
      (*full_cfg.get_key_cert_files()).key_file == "/fake/key_file.key");
    BOOST_TEST(
      (*full_cfg.get_key_cert_files()).cert_file == "/fake/cret_file.crt");
    BOOST_TEST(*full_cfg.get_truststore_file() == "/fake/truststore");
    BOOST_TEST(full_cfg.get_require_client_auth());
}
