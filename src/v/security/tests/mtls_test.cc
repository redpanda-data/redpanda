// Copyright 2022 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0
#include "config/property.h"
#include "random/generators.h"
#include "security/mtls.h"
#include "utils/base64.h"

#include <seastar/testing/thread_test_case.hh>
#include <seastar/util/bool_class.hh>

#include <boost/algorithm/string.hpp>
#include <boost/test/data/monomorphic.hpp>
#include <boost/test/data/test_case.hpp>
#include <boost/test/tools/old/interface.hpp>
#include <boost/test/unit_test.hpp>
#include <boost/test/unit_test_suite.hpp>
#include <fmt/ostream.h>

#include <regex>
#include <stdexcept>

namespace security::tls {

/*
 * Test coverage includes all of the relevant test cases found in upstream
 * kafka's SslPrincipalMapperTest.
 */

namespace bdata = boost::unit_test::data;

std::array<ss::sstring, 8> mtls_valid_rules{
  "DEFAULT",
  "RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/",
  "RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/L, DEFAULT",
  "RULE:^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$/$1@$2/",
  "RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/L",
  "RULE:^cn=(.?),ou=(.?),dc=(.?),dc=(.?)$/$1@$2/U",
  "RULE:^CN=([^,ADEFLTU,]+)(,.*|$)/$1/",
  "RULE:^CN=([^,DEFAULT,]+)(,.*|$)/$1/"};

BOOST_DATA_TEST_CASE(test_mtls_valid_rules, bdata::make(mtls_valid_rules), c) {
    BOOST_REQUIRE_NO_THROW(principal_mapper{
      config::mock_binding(std::optional<std::vector<ss::sstring>>{{c}})});
}

std::array<ss::sstring, 10> mtls_invalid_rules{
  "default",
  "DEFAUL",
  "DEFAULT/L",
  "DEFAULT/U",
  "RULE:CN=(.*?),OU=ServiceUsers.*/$1",
  "rule:^CN=(.*?),OU=ServiceUsers.*$/$1/",
  "RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/L/U",
  "RULE:^CN=(.*?),OU=ServiceUsers.*$/L",
  "RULE:^CN=(.*?),OU=ServiceUsers.*$/U",
  "RULE:^CN=(.*?),OU=ServiceUsers.*$/LU"};

BOOST_DATA_TEST_CASE(
  test_mtls_invalid_rules, bdata::make(mtls_invalid_rules), c) {
    BOOST_REQUIRE_THROW(
      principal_mapper{
        config::mock_binding(std::optional<std::vector<ss::sstring>>{{c}})},
      std::runtime_error);
}

struct record {
    record(std::string_view e, std::string_view i)
      : expected{e}
      , input{i} {}
    std::string_view expected;
    std::string_view input;
    friend std::ostream& operator<<(std::ostream& os, const record& r) {
        fmt::print(os, "input: `{}`, expected: `{}`", r.input, r.expected);
        return os;
    }
};

static std::array<record, 5> mtls_principal_mapper_data{
  record{"duke", "CN=Duke,OU=ServiceUsers,O=Org,C=US"},
  {"duke@sme", "CN=Duke,OU=SME,O=mycp,L=Fulton,ST=MD,C=US"},
  {"DUKE@SME", "cn=duke,ou=sme,dc=mycp,dc=com"},
  {"DUKE", "cN=duke,OU=JavaSoft,O=Sun Microsystems"},
  {"OU=JavaSoft,O=Sun Microsystems,C=US",
   "OU=JavaSoft,O=Sun Microsystems,C=US"}};

BOOST_DATA_TEST_CASE(
  test_mtls_principal_mapper, bdata::make(mtls_principal_mapper_data), c) {
    security::tls::principal_mapper mapper{
      config::mock_binding(std::optional<std::vector<ss::sstring>>{
        {"RULE:^CN=(.*?),OU=ServiceUsers.*$/$1/L, "
         "RULE:^CN=(.*?),OU=(.*?),O=(.*?),L=(.*?),ST=(.*?),C=(.*?)$/$1@$2/L, "
         "RULE:^cn=(.*?),ou=(.*?),dc=(.*?),dc=(.*?)$/$1@$2/U, "
         "RULE:^.*[Cc][Nn]=([a-zA-Z0-9.]*).*$/$1/U, "
         "DEFAULT"}})};
    BOOST_REQUIRE_EQUAL(c.expected, *mapper.apply(c.input));
}

static std::array<record, 18> mtls_rule_splitting_data{
  record{"[]", ""},
  {"[DEFAULT]", "DEFAULT"},
  {"[RULE:/]", "RULE://"},
  {"[RULE:/.*]", "RULE:/.*/"},
  {"[RULE:/.*/L]", "RULE:/.*/L"},
  {"[RULE:/, DEFAULT]", "RULE://,DEFAULT"},
  {"[RULE:/, DEFAULT]", "  RULE:// ,  DEFAULT  "},
  {"[RULE:   /     , DEFAULT]", "  RULE:   /     / ,  DEFAULT  "},
  {"[RULE:  /     /U, DEFAULT]", "  RULE:  /     /U   ,DEFAULT  "},
  {"[RULE:([A-Z]*)/$1/U, RULE:([a-z]+)/$1, DEFAULT]",
   "  RULE:([A-Z]*)/$1/U   ,RULE:([a-z]+)/$1/,   DEFAULT  "},
  {"[]", ",   , , ,      , , ,   "},
  {"[RULE:/, DEFAULT]", ",,RULE://,,,DEFAULT,,"},
  {"[RULE: /   , DEFAULT]", ",  , RULE: /   /    ,,,   DEFAULT, ,   "},
  {"[RULE:   /  /U, DEFAULT]", "     ,  , RULE:   /  /U    ,,  ,DEFAULT, ,"},
  {R"([RULE:\/\\\(\)\n\t/\/\/])", R"(RULE:\/\\\(\)\n\t/\/\//)"},
  {R"([RULE:\**\/+/*/L, RULE:\/*\**/**])",
   R"(RULE:\**\/+/*/L,RULE:\/*\**/**/)"},
  {"[RULE:,RULE:,/,RULE:,\\//U, RULE:,/RULE:,, RULE:,RULE:,/L,RULE:,/L, RULE:, "
   "DEFAULT, /DEFAULT, DEFAULT]",
   "RULE:,RULE:,/,RULE:,\\//U,RULE:,/RULE:,/,RULE:,RULE:,/L,RULE:,/L,RULE:, "
   "DEFAULT, /DEFAULT/,DEFAULT"},
  {"[RULE:/, DEFAULT]", "RULE://\nDEFAULT"},
};
BOOST_DATA_TEST_CASE(
  test_mtls_rule_splitting, bdata::make(mtls_rule_splitting_data), c) {
    BOOST_CHECK_EQUAL(
      c.expected,
      fmt::format(
        "{}",
        principal_mapper(config::mock_binding(
          std::optional<std::vector<ss::sstring>>{{ss::sstring{c.input}}}))));
}

BOOST_AUTO_TEST_CASE(test_mtls_comma_with_whitespace) {
    BOOST_CHECK_EQUAL(
      "Tkac\\, Adam",
      principal_mapper(
        config::mock_binding(std::optional<std::vector<ss::sstring>>{
          {"RULE:^CN=((\\\\, *|\\w)+)(,.*|$)/$1/,DEFAULT"}}))
        .apply("CN=Tkac\\, Adam,OU=ITZ,DC=geodis,DC=cz")
        .value_or(""));
}

BOOST_AUTO_TEST_CASE(test_mtls_parsing_with_multiline) {
    BOOST_CHECK_EQUAL(
      "test_cn",
      principal_mapper(
        config::mock_binding(std::optional<std::vector<ss::sstring>>{
          {{"RULE:^OU=(.*)/$1/"}, {"RULE:^CN=(.*)/$1/"}}}))
        .apply("CN=test_cn")
        .value_or(""));
}

BOOST_AUTO_TEST_CASE(test_mtls_parsing_with_newline) {
    BOOST_CHECK_EQUAL(
      "test_cn",
      principal_mapper(
        config::mock_binding(std::optional<std::vector<ss::sstring>>{
          {"RULE:^OU=(.*)/$1/\nRULE:^CN=(.*)/$1/"}}))
        .apply("CN=test_cn")
        .value_or(""));
}

} // namespace security::tls
