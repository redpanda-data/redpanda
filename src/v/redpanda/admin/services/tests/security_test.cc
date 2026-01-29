/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "proto/redpanda/core/admin/v2/security.proto.h"
#include "redpanda/admin/services/security.h"
#include "security/role.h"
#include "security/scram_algorithm.h"
#include "serde/protobuf/rpc.h"

#include <gtest/gtest.h>

namespace admin {

class SecurityServiceTest : public ::testing::Test {};

// Bring internal namespace into scope for tests
using namespace internal;

// =============================================
// Tests for match_scram_credential
// =============================================

TEST_F(SecurityServiceTest, MatchScramCredentialSha256Valid) {
    ss::sstring password = "test_password";

    // Create SCRAM credential using the algorithm
    auto cred = security::scram_sha256::make_credentials(
      password, security::scram_sha256::min_iterations);

    // Create protobuf SCRAM credential with the same password
    proto::admin::scram_credential pb_cred;
    pb_cred.set_name("test_user");
    pb_cred.set_mechanism(proto::common::scram_mechanism::scram_sha_256);
    pb_cred.set_password(std::move(password));

    // Should match
    EXPECT_TRUE(match_scram_credential(pb_cred, cred));
}

TEST_F(SecurityServiceTest, MatchScramCredentialSha256Invalid) {
    ss::sstring password = "test_password";
    ss::sstring wrong_password = "wrong_password";

    // Create SCRAM credential with the correct password
    auto cred = security::scram_sha256::make_credentials(
      password, security::scram_sha256::min_iterations);

    // Create protobuf SCRAM credential with a wrong password
    proto::admin::scram_credential pb_cred;
    pb_cred.set_name("test_user");
    pb_cred.set_mechanism(proto::common::scram_mechanism::scram_sha_256);
    pb_cred.set_password(std::move(wrong_password));

    // Should not match
    EXPECT_FALSE(match_scram_credential(pb_cred, cred));
}

TEST_F(SecurityServiceTest, MatchScramCredentialSha512Valid) {
    ss::sstring password = "test_password";

    // Create SCRAM credential using the algorithm
    auto cred = security::scram_sha512::make_credentials(
      password, security::scram_sha512::min_iterations);

    // Create protobuf SCRAM credential with the same password
    proto::admin::scram_credential pb_cred;
    pb_cred.set_name("test_user");
    pb_cred.set_mechanism(proto::common::scram_mechanism::scram_sha_512);
    pb_cred.set_password(std::move(password));

    // Should match
    EXPECT_TRUE(match_scram_credential(pb_cred, cred));
}

TEST_F(SecurityServiceTest, MatchScramCredentialSha512Invalid) {
    ss::sstring password = "test_password";
    ss::sstring wrong_password = "wrong_password";

    // Create SCRAM credential with the correct password
    auto cred = security::scram_sha512::make_credentials(
      password, security::scram_sha512::min_iterations);

    // Create protobuf SCRAM credential with a wrong password
    proto::admin::scram_credential pb_cred;
    pb_cred.set_name("test_user");
    pb_cred.set_mechanism(proto::common::scram_mechanism::scram_sha_512);
    pb_cred.set_password(std::move(wrong_password));

    // Should not match
    EXPECT_FALSE(match_scram_credential(pb_cred, cred));
}

TEST_F(SecurityServiceTest, MatchScramCredentialUnknownMechanism) {
    ss::sstring password = "test_password";

    // Create SCRAM credential
    auto cred = security::scram_sha256::make_credentials(
      password, security::scram_sha256::min_iterations);

    // Create protobuf SCRAM credential with an unknown mechanism
    proto::admin::scram_credential pb_cred;
    pb_cred.set_name("test_user");
    pb_cred.set_mechanism(proto::common::scram_mechanism::unspecified);
    pb_cred.set_password(std::move(password));

    // Should throw invalid_argument_exception
    EXPECT_THROW(
      match_scram_credential(pb_cred, cred),
      serde::pb::rpc::invalid_argument_exception);
}

TEST_F(SecurityServiceTest, MatchScramCredentialMismatchedMechanism) {
    ss::sstring password = "test_password";

    // Create SHA-256 SCRAM credential
    auto cred = security::scram_sha256::make_credentials(
      password, security::scram_sha256::min_iterations);

    // Try to validate with SHA-512 mechanism
    proto::admin::scram_credential pb_cred;
    pb_cred.set_name("test_user");
    pb_cred.set_mechanism(proto::common::scram_mechanism::scram_sha_512);
    pb_cred.set_password(std::move(password));

    // Should not match because the mechanisms are different
    EXPECT_FALSE(match_scram_credential(pb_cred, cred));
}

// =============================================
// Tests for validate_scram_credential_name
// =============================================

TEST_F(SecurityServiceTest, ValidateScramCredentialNameValid) {
    // Valid SCRAM credential names should not throw
    EXPECT_NO_THROW(validate_scram_credential_name("admin"));
    EXPECT_NO_THROW(validate_scram_credential_name("user1"));
    EXPECT_NO_THROW(validate_scram_credential_name("my-cred"));
    EXPECT_NO_THROW(validate_scram_credential_name("my_cred"));
    EXPECT_NO_THROW(validate_scram_credential_name("user123"));
}

// Parameterized tests for invalid SCRAM credential names
struct InvalidScramCredentialNameCase {
    ss::sstring name;
    ss::sstring test_suffix;
};

class InvalidScramCredentialNameTest
  : public ::testing::TestWithParam<InvalidScramCredentialNameCase> {};

TEST_P(InvalidScramCredentialNameTest, RejectsInvalidName) {
    EXPECT_THROW(
      validate_scram_credential_name(GetParam().name),
      serde::pb::rpc::invalid_argument_exception);
}

INSTANTIATE_TEST_SUITE_P(
  InvalidNames,
  InvalidScramCredentialNameTest,
  ::testing::Values(
    InvalidScramCredentialNameCase{"user\nname", "newline"},
    InvalidScramCredentialNameCase{"user\tname", "tab"},
    InvalidScramCredentialNameCase{"user\rname", "carriage_return"},
    InvalidScramCredentialNameCase{"\x01user", "control_char"},
    InvalidScramCredentialNameCase{"user,name", "comma"},
    InvalidScramCredentialNameCase{"user=name", "equals"},
    InvalidScramCredentialNameCase{"", "empty"},
    InvalidScramCredentialNameCase{std::string("user\0name", 9), "null_char"}),
  [](const ::testing::TestParamInfo<InvalidScramCredentialNameCase>& info) {
      return info.param.test_suffix;
  });

// =============================================
// Tests for validate_pb_scram_credential
// =============================================

TEST_F(SecurityServiceTest, ValidatePbScramCredentialValidSha256) {
    proto::admin::scram_credential pb_cred;
    pb_cred.set_name("valid_user");
    pb_cred.set_mechanism(proto::common::scram_mechanism::scram_sha_256);
    pb_cred.set_password("a_valid_password_that_is_long_enough");

    // Should not throw
    EXPECT_NO_THROW(validate_pb_scram_credential(pb_cred));
}

TEST_F(SecurityServiceTest, ValidatePbScramCredentialValidSha512) {
    proto::admin::scram_credential pb_cred;
    pb_cred.set_name("valid_user");
    pb_cred.set_mechanism(proto::common::scram_mechanism::scram_sha_512);
    pb_cred.set_password("a_valid_password_that_is_long_enough");

    // Should not throw
    EXPECT_NO_THROW(validate_pb_scram_credential(pb_cred));
}

TEST_F(SecurityServiceTest, ValidatePbScramCredentialInvalidName) {
    proto::admin::scram_credential pb_cred;
    pb_cred.set_name("user\nname"); // Newline is invalid
    pb_cred.set_mechanism(proto::common::scram_mechanism::scram_sha_256);
    pb_cred.set_password("a_valid_password_that_is_long_enough");

    // Should throw due to invalid name
    EXPECT_THROW(
      validate_pb_scram_credential(pb_cred),
      serde::pb::rpc::invalid_argument_exception);
}

TEST_F(SecurityServiceTest, ValidatePbScramCredentialPasswordWithControlChar) {
    proto::admin::scram_credential pb_cred;
    pb_cred.set_name("valid_user");
    pb_cred.set_mechanism(proto::common::scram_mechanism::scram_sha_256);
    pb_cred.set_password("password_with\ncontrol"); // Newline in password

    // Should throw due to control character in password
    EXPECT_THROW(
      validate_pb_scram_credential(pb_cred),
      serde::pb::rpc::invalid_argument_exception);
}

TEST_F(SecurityServiceTest, ValidatePbScramCredentialUnspecifiedMechanism) {
    proto::admin::scram_credential pb_cred;
    pb_cred.set_name("valid_user");
    pb_cred.set_mechanism(proto::common::scram_mechanism::unspecified);
    pb_cred.set_password("a_valid_password_that_is_long_enough");

    // Should throw due to unspecified mechanism
    EXPECT_THROW(
      validate_pb_scram_credential(pb_cred),
      serde::pb::rpc::invalid_argument_exception);
}

// =============================================
// Tests for convert_to_security_scram_credential
// =============================================

TEST_F(SecurityServiceTest, ConvertToSecurityScramCredentialSha256) {
    proto::admin::scram_credential pb_cred;
    pb_cred.set_name("test_user");
    pb_cred.set_mechanism(proto::common::scram_mechanism::scram_sha_256);
    pb_cred.set_password("test_password");

    auto security_cred = convert_to_security_scram_credential(pb_cred);

    // Verify the credential was created with correct properties
    EXPECT_EQ(
      security_cred.iterations(), security::scram_sha256::min_iterations);
    EXPECT_FALSE(security_cred.salt().empty());
    EXPECT_FALSE(security_cred.stored_key().empty());
    EXPECT_FALSE(security_cred.server_key().empty());

    EXPECT_TRUE(match_scram_credential(pb_cred, security_cred));
}

TEST_F(SecurityServiceTest, ConvertToSecurityScramCredentialSha512) {
    proto::admin::scram_credential pb_cred;
    pb_cred.set_name("test_user");
    pb_cred.set_mechanism(proto::common::scram_mechanism::scram_sha_512);
    pb_cred.set_password("test_password");

    auto security_cred = convert_to_security_scram_credential(pb_cred);

    // Verify the credential was created with correct properties
    EXPECT_EQ(
      security_cred.iterations(), security::scram_sha512::min_iterations);
    EXPECT_FALSE(security_cred.salt().empty());
    EXPECT_FALSE(security_cred.stored_key().empty());
    EXPECT_FALSE(security_cred.server_key().empty());

    EXPECT_TRUE(match_scram_credential(pb_cred, security_cred));
}

TEST_F(SecurityServiceTest, ConvertToSecurityScramCredentialUnknownMechanism) {
    proto::admin::scram_credential pb_cred;
    pb_cred.set_name("test_user");
    pb_cred.set_mechanism(proto::common::scram_mechanism::unspecified);
    pb_cred.set_password("test_password");

    // Should throw due to unknown mechanism
    EXPECT_THROW(
      convert_to_security_scram_credential(pb_cred),
      serde::pb::rpc::invalid_argument_exception);
}

// =============================================
// Tests for convert_to_pb_scram_credential
// =============================================

TEST_F(SecurityServiceTest, ConvertToPbScramCredentialSha256) {
    ss::sstring password = "test_password";
    ss::sstring name = "test_user";

    // Create a SHA-256 SCRAM credential
    auto security_cred = security::scram_sha256::make_credentials(
      password, security::scram_sha256::min_iterations);

    // Convert to protobuf
    auto pb_cred = convert_to_pb_scram_credential(name, security_cred);

    // Verify the mechanism is set correctly
    EXPECT_EQ(
      pb_cred.get_mechanism(), proto::common::scram_mechanism::scram_sha_256);
    EXPECT_EQ(pb_cred.get_name(), name);

    // The password field is not able to be populated during conversion.
    // Therefore, the match should fail.
    EXPECT_TRUE(pb_cred.get_password().empty());
    EXPECT_FALSE(match_scram_credential(pb_cred, security_cred));

    // Verify password_set_at is populated with a real timestamp
    EXPECT_GT(pb_cred.get_password_set_at(), absl::UnixEpoch());
}

TEST_F(SecurityServiceTest, ConvertToPbScramCredentialSha512) {
    ss::sstring password = "test_password";
    ss::sstring name = "test_user";

    // Create a SHA-512 SCRAM credential
    auto security_cred = security::scram_sha512::make_credentials(
      password, security::scram_sha512::min_iterations);

    // Convert to protobuf
    auto pb_cred = convert_to_pb_scram_credential(name, security_cred);

    // Verify the mechanism is set correctly
    EXPECT_EQ(
      pb_cred.get_mechanism(), proto::common::scram_mechanism::scram_sha_512);
    EXPECT_EQ(pb_cred.get_name(), name);

    // The password field is not able to be populated during conversion.
    // Therefore, the match should fail.
    EXPECT_TRUE(pb_cred.get_password().empty());
    EXPECT_FALSE(match_scram_credential(pb_cred, security_cred));

    // Verify password_set_at is populated with a real timestamp
    EXPECT_GT(pb_cred.get_password_set_at(), absl::UnixEpoch());
}

TEST_F(SecurityServiceTest, ConvertToPbScramCredentialWithoutTimestamp) {
    ss::sstring name = "test_user";
    ss::sstring password = "test_password";

    // Create a SHA-256 SCRAM credential with a missing password_set_at
    auto security_cred = security::scram_sha256::make_credentials(
      password,
      security::scram_sha256::min_iterations,
      model::timestamp::missing());

    // Convert to protobuf
    auto pb_cred = convert_to_pb_scram_credential(name, security_cred);

    // Verify the mechanism is set correctly
    EXPECT_EQ(
      pb_cred.get_mechanism(), proto::common::scram_mechanism::scram_sha_256);
    EXPECT_EQ(pb_cred.get_name(), name);

    // Verify password_set_at is set to UnixEpoch for old credentials without
    // a timestamp.
    EXPECT_EQ(pb_cred.get_password_set_at(), absl::UnixEpoch());
}

TEST_F(SecurityServiceTest, ConvertToPbScramCredentialUnknownKeySize) {
    ss::sstring name = "test_user";

    // Create a credential with an invalid stored key size
    // Using empty keys which will have size 0 (not matching SHA-256 or SHA-512)
    security::scram_credential invalid_cred{
      bytes{}, // salt
      bytes{}, // server_key
      bytes{}, // stored_key - empty, so size = 0
      security::scram_sha256::min_iterations};

    // Should throw internal_exception due to unknown key size
    EXPECT_THROW(
      convert_to_pb_scram_credential(name, invalid_cred),
      serde::pb::rpc::internal_exception);
}

// =============================================
// Tests for validate_role_name
// =============================================

TEST_F(SecurityServiceTest, ValidateRoleNameValid) {
    // Valid role names should not throw
    EXPECT_NO_THROW(validate_role_name("admin"));
    EXPECT_NO_THROW(validate_role_name("user1"));
    EXPECT_NO_THROW(validate_role_name("my-role"));
    EXPECT_NO_THROW(validate_role_name("my_role"));
    EXPECT_NO_THROW(validate_role_name("role123"));
}

// Parameterized tests for invalid role names
struct InvalidRoleNameCase {
    ss::sstring name;
    ss::sstring test_suffix;
};

class InvalidRoleNameTest
  : public ::testing::TestWithParam<InvalidRoleNameCase> {};

TEST_P(InvalidRoleNameTest, RejectsInvalidName) {
    EXPECT_THROW(
      validate_role_name(GetParam().name),
      serde::pb::rpc::invalid_argument_exception);
}

INSTANTIATE_TEST_SUITE_P(
  InvalidNames,
  InvalidRoleNameTest,
  ::testing::Values(
    InvalidRoleNameCase{"role\nname", "newline"},
    InvalidRoleNameCase{"role\tname", "tab"},
    InvalidRoleNameCase{"role\rname", "carriage_return"},
    InvalidRoleNameCase{"\x01role", "control_char"},
    InvalidRoleNameCase{"role,name", "comma"},
    InvalidRoleNameCase{"role=name", "equals"},
    InvalidRoleNameCase{"", "empty"}),
  [](const ::testing::TestParamInfo<InvalidRoleNameCase>& info) {
      return info.param.test_suffix;
  });

// =============================================
// Tests for validate_pb_role_member
// =============================================

TEST_F(SecurityServiceTest, ValidatePbRoleMemberValidUser) {
    proto::admin::role_user pb_user;
    pb_user.set_name("alice");
    proto::admin::role_member pb_member;
    pb_member.set_user(std::move(pb_user));

    EXPECT_NO_THROW(validate_pb_role_member(pb_member));
}

TEST_F(SecurityServiceTest, ValidatePbRoleMemberValidGroup) {
    proto::admin::role_group pb_group;
    pb_group.set_name("admins");
    proto::admin::role_member pb_member;
    pb_member.set_group(std::move(pb_group));

    EXPECT_NO_THROW(validate_pb_role_member(pb_member));
}

TEST_F(SecurityServiceTest, ValidatePbRoleMemberWithControlCharacters) {
    proto::admin::role_user pb_user;
    pb_user.set_name("alice\nsmith");
    proto::admin::role_member pb_member;
    pb_member.set_user(std::move(pb_user));

    EXPECT_THROW(
      validate_pb_role_member(pb_member),
      serde::pb::rpc::invalid_argument_exception);
}

TEST_F(SecurityServiceTest, ValidatePbRoleMemberEmptyMember) {
    proto::admin::role_member pb_member;

    EXPECT_THROW(
      validate_pb_role_member(pb_member),
      serde::pb::rpc::invalid_argument_exception);
}

// =============================================
// Tests for convert_to_security_role_member
// =============================================

TEST_F(SecurityServiceTest, ConvertToSecurityRoleMemberUser) {
    proto::admin::role_user pb_user;
    pb_user.set_name("alice");

    proto::admin::role_member pb_member;
    pb_member.set_user(std::move(pb_user));

    auto security_member = convert_to_security_role_member(pb_member);

    EXPECT_EQ(security_member.type(), security::role_member_type::user);
    EXPECT_EQ(security_member.name(), "alice");
}

TEST_F(SecurityServiceTest, ConvertToSecurityRoleMemberGroup) {
    proto::admin::role_group pb_group;
    pb_group.set_name("admins");

    proto::admin::role_member pb_member;
    pb_member.set_group(std::move(pb_group));

    auto security_member = convert_to_security_role_member(pb_member);

    EXPECT_EQ(security_member.type(), security::role_member_type::group);
    EXPECT_EQ(security_member.name(), "admins");
}

TEST_F(SecurityServiceTest, ConvertToSecurityRoleMemberEmptyMember) {
    proto::admin::role_member pb_member;

    EXPECT_THROW(
      convert_to_security_role_member(pb_member),
      serde::pb::rpc::unknown_exception);
}

// =============================================
// Tests for convert_to_security_role
// =============================================

TEST_F(SecurityServiceTest, ConvertToSecurityRoleEmpty) {
    proto::admin::role pb_role;
    pb_role.set_name("empty_role");

    auto security_role = convert_to_security_role(pb_role);

    EXPECT_TRUE(security_role.members().empty());
}

TEST_F(SecurityServiceTest, ConvertToSecurityRoleSingleMember) {
    proto::admin::role pb_role;
    pb_role.set_name("single_member_role");

    proto::admin::role_user pb_user;
    pb_user.set_name("alice");
    proto::admin::role_member pb_member;
    pb_member.set_user(std::move(pb_user));

    auto& members = pb_role.get_members();
    members.push_back(std::move(pb_member));

    auto security_role = convert_to_security_role(pb_role);

    EXPECT_EQ(security_role.members().size(), 1);
    auto it = security_role.members().begin();
    EXPECT_EQ(it->name(), "alice");
    EXPECT_EQ(it->type(), security::role_member_type::user);
}

TEST_F(SecurityServiceTest, ConvertToSecurityRoleMultipleMembers) {
    proto::admin::role pb_role;
    pb_role.set_name("multi_member_role");

    auto& members = pb_role.get_members();

    // Add alice
    proto::admin::role_user pb_user1;
    pb_user1.set_name("alice");
    proto::admin::role_member pb_member1;
    pb_member1.set_user(std::move(pb_user1));
    members.push_back(std::move(pb_member1));

    // Add bob
    proto::admin::role_user pb_user2;
    pb_user2.set_name("bob");
    proto::admin::role_member pb_member2;
    pb_member2.set_user(std::move(pb_user2));
    members.push_back(std::move(pb_member2));

    // Add charlie
    proto::admin::role_user pb_user3;
    pb_user3.set_name("charlie");
    proto::admin::role_member pb_member3;
    pb_member3.set_user(std::move(pb_user3));
    members.push_back(std::move(pb_member3));

    auto security_role = convert_to_security_role(pb_role);

    EXPECT_EQ(security_role.members().size(), 3);

    // Check that all members are present (order may vary due to hash set)
    std::vector<ss::sstring> member_names;
    for (const auto& member : security_role.members()) {
        member_names.push_back(ss::sstring{member.name()});
    }
    std::ranges::sort(member_names);

    EXPECT_EQ(member_names[0], "alice");
    EXPECT_EQ(member_names[1], "bob");
    EXPECT_EQ(member_names[2], "charlie");
}

TEST_F(SecurityServiceTest, ConvertToSecurityRoleMixedMembers) {
    proto::admin::role pb_role;
    pb_role.set_name("mixed_member_role");

    auto& members = pb_role.get_members();

    // Add user
    proto::admin::role_user pb_user;
    pb_user.set_name("alice");
    proto::admin::role_member pb_member_user;
    pb_member_user.set_user(std::move(pb_user));
    members.push_back(std::move(pb_member_user));

    // Add group
    proto::admin::role_group pb_group;
    pb_group.set_name("admins");
    proto::admin::role_member pb_member_group;
    pb_member_group.set_group(std::move(pb_group));
    members.push_back(std::move(pb_member_group));

    auto security_role = convert_to_security_role(pb_role);

    // Verify we have exactly the expected members
    security::role::container_type expected;
    expected.emplace(security::role_member_type::user, "alice");
    expected.emplace(security::role_member_type::group, "admins");

    EXPECT_EQ(security_role.members(), expected);
}

// =============================================
// Tests for convert_to_pb_role_member
// =============================================

TEST_F(SecurityServiceTest, ConvertToPbRoleMemberUser) {
    security::role_member security_member{
      security::role_member_type::user, "alice"};

    auto pb_member = convert_to_pb_role_member(security_member);

    EXPECT_TRUE(pb_member.has_user());
    EXPECT_EQ(pb_member.get_user().get_name(), "alice");
}

TEST_F(SecurityServiceTest, ConvertToPbRoleMemberGroup) {
    security::role_member security_member{
      security::role_member_type::group, "admins"};

    auto pb_member = convert_to_pb_role_member(security_member);

    EXPECT_TRUE(pb_member.has_group());
    EXPECT_EQ(pb_member.get_group().get_name(), "admins");
}

TEST_F(SecurityServiceTest, ConvertToPbRoleMemberUnknownType) {
    security::role_member security_member{
      static_cast<security::role_member_type>(-1), "unknown"};

    EXPECT_THROW(
      convert_to_pb_role_member(security_member),
      serde::pb::rpc::internal_exception);
}

// =============================================
// Tests for convert_to_pb_role
// =============================================

TEST_F(SecurityServiceTest, ConvertToPbRoleEmpty) {
    security::role security_role{};

    auto pb_role = convert_to_pb_role("empty_role", security_role);

    EXPECT_EQ(pb_role.get_name(), "empty_role");
    EXPECT_TRUE(pb_role.get_members().empty());
}

TEST_F(SecurityServiceTest, ConvertToPbRoleSingleMember) {
    security::role::container_type members;
    members.emplace(security::role_member_type::user, "alice");
    security::role security_role{std::move(members)};

    auto pb_role = convert_to_pb_role("single_member_role", security_role);

    EXPECT_EQ(pb_role.get_name(), "single_member_role");
    EXPECT_EQ(pb_role.get_members().size(), 1);

    const auto& pb_member = pb_role.get_members()[0];
    EXPECT_TRUE(pb_member.has_user());
    EXPECT_EQ(pb_member.get_user().get_name(), "alice");
}

TEST_F(SecurityServiceTest, ConvertToPbRoleMultipleMembers) {
    security::role::container_type members;
    members.emplace(security::role_member_type::user, "alice");
    members.emplace(security::role_member_type::user, "bob");
    members.emplace(security::role_member_type::user, "charlie");
    security::role security_role{std::move(members)};

    auto pb_role = convert_to_pb_role("multi_member_role", security_role);

    EXPECT_EQ(pb_role.get_name(), "multi_member_role");
    EXPECT_EQ(pb_role.get_members().size(), 3);

    // Collect member names (order may vary due to hash set)
    std::vector<ss::sstring> member_names;
    for (const auto& pb_member : pb_role.get_members()) {
        EXPECT_TRUE(pb_member.has_user());
        member_names.push_back(pb_member.get_user().get_name());
    }
    std::ranges::sort(member_names);

    EXPECT_EQ(member_names[0], "alice");
    EXPECT_EQ(member_names[1], "bob");
    EXPECT_EQ(member_names[2], "charlie");
}

// =============================================
// Tests for round-trip conversions
// =============================================

TEST_F(SecurityServiceTest, RoundTripConversionSingleMember) {
    // Create protobuf role
    proto::admin::role pb_role_original;
    pb_role_original.set_name("test_role");

    proto::admin::role_user pb_user;
    pb_user.set_name("alice");
    proto::admin::role_member pb_member;
    pb_member.set_user(std::move(pb_user));

    auto& members = pb_role_original.get_members();
    members.push_back(std::move(pb_member));

    // Convert to security role
    auto security_role = convert_to_security_role(pb_role_original);

    // Convert back to protobuf
    auto pb_role_final = convert_to_pb_role("test_role", security_role);

    // Verify round-trip
    EXPECT_EQ(pb_role_final.get_name(), pb_role_original.get_name());
    EXPECT_EQ(
      pb_role_final.get_members().size(),
      pb_role_original.get_members().size());

    const auto& member_final = pb_role_final.get_members()[0];
    EXPECT_TRUE(member_final.has_user());
    EXPECT_EQ(member_final.get_user().get_name(), "alice");
}

TEST_F(SecurityServiceTest, RoundTripConversionMultipleMembers) {
    // Create protobuf role with multiple members
    proto::admin::role pb_role_original;
    pb_role_original.set_name("test_role");

    auto& members = pb_role_original.get_members();

    std::vector<ss::sstring> names = {"alice", "bob", "charlie", "dave"};
    for (const auto& name : names) {
        proto::admin::role_user pb_user;
        pb_user.set_name(ss::sstring{name});
        proto::admin::role_member pb_member;
        pb_member.set_user(std::move(pb_user));
        members.push_back(std::move(pb_member));
    }

    // Convert to security role and back
    auto security_role = convert_to_security_role(pb_role_original);
    auto pb_role_final = convert_to_pb_role("test_role", security_role);

    // Verify member count
    EXPECT_EQ(pb_role_final.get_name(), pb_role_original.get_name());
    EXPECT_EQ(
      pb_role_final.get_members().size(),
      pb_role_original.get_members().size());

    // Collect and sort names
    std::vector<ss::sstring> original_names;
    for (const auto& pb_member : pb_role_original.get_members()) {
        original_names.push_back(pb_member.get_user().get_name());
    }
    std::ranges::sort(original_names);

    std::vector<ss::sstring> final_names;
    for (const auto& pb_member : pb_role_final.get_members()) {
        EXPECT_TRUE(pb_member.has_user());
        final_names.push_back(pb_member.get_user().get_name());
    }
    std::ranges::sort(final_names);

    EXPECT_EQ(final_names, original_names);
}

TEST_F(SecurityServiceTest, RoundTripConversionEmptyRole) {
    // Create empty protobuf role
    proto::admin::role pb_role_original;
    pb_role_original.set_name("empty_role");

    // Convert to security role and back
    auto security_role = convert_to_security_role(pb_role_original);
    auto pb_role_final = convert_to_pb_role("empty_role", security_role);

    // Verify round-trip
    EXPECT_EQ(pb_role_final.get_name(), pb_role_original.get_name());
    EXPECT_TRUE(pb_role_final.get_members().empty());
    EXPECT_EQ(pb_role_original, pb_role_final);
}

// =============================================
// Tests for member deduplication
// =============================================

TEST_F(SecurityServiceTest, SecurityRoleDeduplicatesMembers) {
    // Create protobuf role with duplicate members
    proto::admin::role pb_role;
    pb_role.set_name("role_with_duplicates");

    auto& members = pb_role.get_members();

    // Add alice twice
    for (int i = 0; i < 2; ++i) {
        proto::admin::role_user pb_user;
        pb_user.set_name("alice");
        proto::admin::role_member pb_member;
        pb_member.set_user(std::move(pb_user));
        members.push_back(std::move(pb_member));
    }

    // Add bob
    proto::admin::role_user pb_user_bob;
    pb_user_bob.set_name("bob");
    proto::admin::role_member pb_member_bob;
    pb_member_bob.set_user(std::move(pb_user_bob));
    members.push_back(std::move(pb_member_bob));

    // Convert to security role
    auto security_role = convert_to_security_role(pb_role);

    // Verify deduplication
    EXPECT_EQ(security_role.members().size(), 2);

    std::vector<ss::sstring> member_names;
    for (const auto& member : security_role.members()) {
        member_names.emplace_back(member.name());
    }
    std::ranges::sort(member_names);

    EXPECT_EQ(member_names[0], "alice");
    EXPECT_EQ(member_names[1], "bob");
}

TEST_F(SecurityServiceTest, SecurityRoleDistinguishesUserAndGroupWithSameName) {
    proto::admin::role pb_role;
    pb_role.set_name("role_same_name");

    auto& members = pb_role.get_members();

    // Add user "admin"
    proto::admin::role_user pb_user;
    pb_user.set_name("admin");
    proto::admin::role_member pb_member_user;
    pb_member_user.set_user(std::move(pb_user));
    members.push_back(std::move(pb_member_user));

    // Add group "admin"
    proto::admin::role_group pb_group;
    pb_group.set_name("admin");
    proto::admin::role_member pb_member_group;
    pb_member_group.set_group(std::move(pb_group));
    members.push_back(std::move(pb_member_group));

    auto security_role = convert_to_security_role(pb_role);

    // Should have 2 distinct members despite same name
    security::role::container_type expected;
    expected.emplace(security::role_member_type::user, "admin");
    expected.emplace(security::role_member_type::group, "admin");

    EXPECT_EQ(security_role.members(), expected);
}

} // namespace admin
