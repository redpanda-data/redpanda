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

#pragma once

#include "crypto_test_utils.h"

namespace test_values {

// NOLINTBEGIN
inline constexpr std::string_view md5_test_val
  = "The quick brown fox jumps over the lazy dog";

const auto md5_expected_val = convert_from_hex(
  "9e107d9d372bb6826bd81d3542a419d6");

const auto sha256_test_val = convert_from_hex(
  "09fc1accc230a205e4a208e64a8f204291f581a12756392da4b8c0cf5ef02b95");

const auto sha256_expected_val = convert_from_hex(
  "4f44c1c7fbebb6f9601829f3897bfd650c56fa07844be76489076356ac1886a4");

const auto sha512_test_val = convert_from_hex(
  "8ccb08d2a1a282aa8cc99902ecaf0f67a9f21cffe28005cb27fcf129e963f99d");

const auto sha512_expected_val = convert_from_hex(
  "4551def2f9127386eea8d4dae1ea8d8e49b2add0509f27ccbce7d9e950ac7db01d5bca579c27"
  "1b9f2d806730d88f58252fd0c2587851c3ac8a0e72b4e1dc0da6");

const auto hmac_sha256_key = convert_from_hex(
  "9779d9120642797f1747025d5b22b7ac607cab08e1758f2f3a46c8be1e25c53b8c6a8f58ffef"
  "a176");

const auto hmac_sha256_msg = convert_from_hex(
  "b1689c2591eaf3c9e66070f8a77954ffb81749f1b00346f9dfe0b2ee905dcc288baf4a92de3f"
  "4001dd9f44c468c3d07d6c6ee82faceafc97c2fc0fc0601719d2dcd0aa2aec92d1b0ae933c65"
  "eb06a03c9c935c2bad0459810241347ab87e9f11adb30415424c6c7f5f22a003b8ab8de54f6d"
  "ed0e3ab9245fa79568451dfa258e");

const auto hmac_sha256_expected = convert_from_hex(
  "769f00d3e6a6cc1fb426a14a4f76c6462e6149726e0dee0ec0cf97a16605ac8b");

const auto hmac_sha512_key = convert_from_hex(
  "57c2eb677b5093b9e829ea4babb50bde55d0ad59fec34a618973802b2ad9b78e26b2045dda78"
  "4df3ff90ae0f2cc51ce39cf54867320ac6f3ba2c6f0d72360480c96614ae66581f266c35fb79"
  "fd28774afd113fa5187eff9206d7cbe90dd8bf67c844e202");

const auto hmac_sha512_msg = convert_from_hex(
  "2423dff48b312be864cb3490641f793d2b9fb68a7763b8e298c86f42245e4540eb01ae4d2d45"
  "00370b1886f23ca2cf9701704cad5bd21ba87b811daf7a854ea24a56565ced425b35e40e1acb"
  "ebe03603e35dcf4a100e57218408a1d8dbcc3b99296cfea931efe3ebd8f719a6d9a15487b9ad"
  "67eafedf15559ca42445b0f9b42e");

const auto hmac_sha512_expected = convert_from_hex(
  "33c511e9bc2307c62758df61125a980ee64cefebd90931cb91c13742d4714c06de4003faf3c4"
  "1c06aefc638ad47b21906e6b104816b72de6269e045a1f4429d4");

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

const auto sig_test_rsa_pub_key_n = convert_from_hex(
  "c47abacc2a84d56f3614d92fd62ed36ddde459664b9301dcd1d61781cfcc026bcb2399bee7e7"
  "5681a80b7bf500e2d08ceae1c42ec0b707927f2b2fe92ae852087d25f1d260cc74905ee5f9b2"
  "54ed05494a9fe06732c3680992dd6f0dc634568d11542a705f83ae96d2a49763d5fbb24398ed"
  "f3702bc94bc168190166492b8671de874bb9cecb058c6c8344aa8c93754d6effcd44a41ed7de"
  "0a9dcd9144437f212b18881d042d331a4618a9e630ef9bb66305e4fdf8f0391b3b2313fe549f"
  "0189ff968b92f33c266a4bc2cffc897d1937eeb9e406f5d0eaa7a14782e76af3fce98f54ed23"
  "7b4a04a4159a5f6250a296a902880204e61d891c4da29f2d65f34cbb");

const auto sig_test_rsa_pub_key_e = convert_from_hex("49d2a1");

const auto sig_good_msg = convert_from_hex(
  "95123c8d1b236540b86976a11cea31f8bd4e6c54c235147d20ce722b03a6ad756fbd918c27df"
  "8ea9ce3104444c0bbe877305bc02e35535a02a58dcda306e632ad30b3dc3ce0ba97fdf46ec19"
  "2965dd9cd7f4a71b02b8cba3d442646eeec4af590824ca98d74fbca934d0b6867aa1991f3040"
  "b707e806de6e66b5934f05509bea");

const auto sig_good_sig = convert_from_hex(
  "51265d96f11ab338762891cb29bf3f1d2b3305107063f5f3245af376dfcc7027d39365de70a3"
  "1db05e9e10eb6148cb7f6425f0c93c4fb0e2291adbd22c77656afc196858a11e1c670d9eeb59"
  "2613e69eb4f3aa501730743ac4464486c7ae68fd509e896f63884e9424f69c1c5397959f1e52"
  "a368667a598a1fc90125273d9341295d2f8e1cc4969bf228c860e07a3546be2eeda1cde48ee9"
  "4d062801fe666e4a7ae8cb9cd79262c017b081af874ff00453ca43e34efdb43fffb0bb42a4e2"
  "d32a5e5cc9e8546a221fe930250e5f5333e0efe58ffebf19369a3b8ae5a67f6a048bc9ef915b"
  "da25160729b508667ada84a0c27e7e26cf2abca413e5e4693f4a9405");

const auto sig_bad_msg = convert_from_hex(
  "f89fd2f6c45a8b5066a651410b8e534bfec0d9a36f3e2b887457afd44dd651d1ec79274db5a4"
  "55f182572fceea5e9e39c3c7c5d9e599e4fe31c37c34d253b419c3e8fb6b916aef6563f87d4c"
  "37224a456e5952698ba3d01b38945d998a795bd285d69478e3131f55117284e27b441f16095d"
  "ca7ce9c5b68890b09a2bfbb010a5");

const auto sig_bad_sig = convert_from_hex(
  "ba48538708512d45c0edcac57a9b4fb637e9721f72003c60f13f5c9a36c968cef9be8f546654"
  "18141c3d9ecc02a5bf952cfc055fb51e18705e9d8850f4e1f5a344af550de84ffd0805e27e55"
  "7f6aa50d2645314c64c1c71aa6bb44faf8f29ca6578e2441d4510e36052f46551df341b2dcf4"
  "3f761f08b946ca0b7081dadbb88e955e820fd7f657c4dd9f4554d167dd7c9a487ed41ced2b40"
  "068098deedc951060faf7e15b1f0f80ae67ff2ee28a238d80bf72dd71c8d95c79bc156114ece"
  "8ec837573a4b66898d45b45a5eacd0b0e41447d8fa08a367f437645e50c9920b88a16bc08801"
  "47acfb9a79de9e351b3fa00b3f4e9f182f45553dffca55e393c5eab6");
// NOLINTEND
} // namespace test_values
