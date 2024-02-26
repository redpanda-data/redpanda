/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "crypto/crypto.h"
#include "crypto/exceptions.h"
#include "crypto/types.h"
#include "crypto_test_utils.h"
#include "gtest/gtest.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

#include <stdexcept>

// NOLINTBEGIN
static const ss::sstring example_pem_rsa_private_key
  = R"(-----BEGIN PRIVATE KEY-----
MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQCncXJOCFKCBJYm
QEtoNPe1tcNR0aNB51JXjW24Rp07SVEY9Q8m4VLUfRhFPsB630F3F93eGa7WB9To
LlC7m4co5Z3LZeLE5JG3DgVEN2PGI90HbOepfh+YQxmq3Bw9qKtlHVt1aTQbZVIr
rrpQj2npOIpzUDm2ji1h1RUAgRYCmIExbg306RvqeTBOqhTI7tyzMFsQjZcQAIv1
tpQ6VHb+sBmQMBCRzh2OhPPu4ZkOxXjqGryRoXZa1/jXzVpcDIotRpmZaWHQRh37
E5ZKIhrku0Xae3DmZoQTrn3dLCT4s4il3yugS+9MD1su8IV/TPxj7DoucG0QQUnL
DYlYc9W/AgMBAAECggEANz6ERn+TbUdDHMqwtmhnY+Hc1+VRNmCyN6W3UgmmPZXC
dnf/8EV+NRIyzEHYcpGvQTI0Jt+VYhNCaPpC86rsLI+ZgK6UY37AHsO29BtMRWa2
uYjyY+bzWKKm2Mr3XFaGef12G+ZCZVmIA1aKLSMr/+ECOOp6qCL/kRwi6kAsuVz7
/VKcOB7iFVmDiBgklH6agD2hcjD71jHl+RlTCxOzkC7ilerSr3uV3vU6HACjCklD
wpZoNFXBgX8qTSz/rpoKzlE4KViw6iYdBPkE08UdfOf2Rd5W+ITGWlbYV/y78D6D
xJuwb07TarOE1555FKSRj66HOy5vz31m5avXP3KNiQKBgQDR7s/79Ss2id1e6NTz
EFXSYLL/pdxG77WcOmSNGL2xAQN6fkdWQDpUKPeIqpflfG5vR6Rx1SA52bHZrSBT
PMuVsaKokcwTIIOxebzmXMDA74LK8cCcka/CLByV1dZDlkct28e440WatPYZkP+O
QnNf7xmZLi1z9XybD/tO96oDeQKBgQDML7t0m4xzpm5bCW3P5YPIUx2Yy4FVQCNj
9dbyzc2/oOgHZW6bnXuwaKvE0XKTrsxwEOMlndjMIfOEH8kwlrxJ0upEvYM+2OC2
mI4y/MEr5uO1AG/mJShV4SOEuINQwM73QlEF9PgjWvLgxJ+H/H0YQNih8JWHtmJo
8qAVjcNc9wKBgQCst504P2J5MX4G2upwu+zP9CzwteYAGrHBQi1+BG/0k8/n1MMe
TCNxIG9fanMkJHa7aSb7XIxx7BAt9gkVUnxwwUABDkrnJaYTuwPWR1NyqNtj2vhM
GHSQ/Tfbcp4g5x/Ss/Kiw6F9ggrDyA7pXPSNZisaYuqUb9E/xitNseeXiQKBgQDH
MTGQWka0dAJocVRdYiwje2H+M1mijwV3eNcO21MCxLhWrs8upH2L5TDcuu8pv3bV
RMQzaD+dNOnZVSDyc7qP0mCUWsT0xKLDvyPJ/eV9LKurYhfHzywAS7hYu5/vYYkG
kf108Dw6UXlraKWxBdILnQc5Q/i8AmMSus8M99VElQKBgQCo33Lgq/8RvmIXeolj
llAP2nTLBdubDka/q4xBaklSAOwBOunmfW43C9VNhrVpMxEohgSkG77L6ZNfYjmt
CSuBq0QNxNf1hEjVOzd9TdqMGHJ16I6RJHpaFGZyKa7mY6ds7V5KyiMrLQ3UWETY
Pu74Px6xOVztryyCGWU5V2jgLg==
-----END PRIVATE KEY-----)";

static const ss::sstring example_pem_rsa_public_key
  = R"(-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAp3FyTghSggSWJkBLaDT3
tbXDUdGjQedSV41tuEadO0lRGPUPJuFS1H0YRT7Aet9Bdxfd3hmu1gfU6C5Qu5uH
KOWdy2XixOSRtw4FRDdjxiPdB2znqX4fmEMZqtwcPairZR1bdWk0G2VSK666UI9p
6TiKc1A5to4tYdUVAIEWApiBMW4N9Okb6nkwTqoUyO7cszBbEI2XEACL9baUOlR2
/rAZkDAQkc4djoTz7uGZDsV46hq8kaF2Wtf4181aXAyKLUaZmWlh0EYd+xOWSiIa
5LtF2ntw5maEE6593Swk+LOIpd8roEvvTA9bLvCFf0z8Y+w6LnBtEEFJyw2JWHPV
vwIDAQAB
-----END PUBLIC KEY-----)";

static const ss::sstring example_pem_ec_public_key
  = R"(-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE+oxV254t9JXTgcR2VlCwyiUhsfHS
pBo8R0hyKF8DjuMn49KNqfL2Aq9jXtr8l7JVfBaMGu1rSgch1OhiC2Lw1Q==
-----END PUBLIC KEY-----)";

const auto rsa_pub_key_n = convert_from_hex(
  "ddc1676352ca011a235db9b4bb41eab81a9f3447a34c3626a531e3319665edd9c9e269788323"
  "ac7f2db36b9106f4b2148b7c7a309a0b7482ff08cc97c792bf8e2319f42aa51078a29a4ff90c"
  "0e29563059a8608e8809a04bf45f1334b23631d99253ba230dc640ffc3a70c27ce5fc7ebd1ad"
  "fe68e4462790007b39f5d5b47dd9bd04d0d08ac3b586fd6cc8e178d52ecbc09434d4b89d83ca"
  "def6c53cce17788e87b551aa0b507893f308e23da919a4aa01183ddc831a99a3e3c4e5bffdc7"
  "e8c8b6800699abdf11569ba66e5892b2e55c6f8578a12f5e304dc28ffbd5ee2dfd2bafabac77"
  "ba67031f588e73cf7ba344396d166f5392ad36187b45e15916aaf5b7");

const auto rsa_pub_key_e = convert_from_hex("748d77");
// NOLINTEND

TEST(crypto_key, load_pem_private_key) {
    auto key = crypto::key::load_key(
      example_pem_rsa_private_key,
      crypto::format_type::PEM,
      crypto::is_private_key_t::yes);

    ASSERT_EQ(key.get_type(), crypto::key_type::RSA);
    ASSERT_TRUE(key.is_private_key());
}

TEST(crypto_key, load_pem_public_key) {
    auto key = crypto::key::load_key(
      example_pem_rsa_public_key,
      crypto::format_type::PEM,
      crypto::is_private_key_t::no);

    ASSERT_EQ(key.get_type(), crypto::key_type::RSA);
    ASSERT_FALSE(key.is_private_key());
}

TEST(crypto_key, load_ec_key) {
    EXPECT_THROW(
      crypto::key::load_key(
        example_pem_ec_public_key,
        crypto::format_type::PEM,
        crypto::is_private_key_t::no),
      crypto::exception);
}

TEST(crypto_key, load_rsa_pub_key_components) {
    ASSERT_NO_THROW(
      crypto::key::load_rsa_public_key(rsa_pub_key_n, rsa_pub_key_e));
}
