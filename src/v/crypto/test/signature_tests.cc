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
#include "crypto/types.h"
#include "crypto_test_utils.h"
#include "test_utils/test.h"

#include <gtest/gtest.h>

// NOLINTBEGIN
const auto rsa_pub_key_n = convert_from_hex(
  "c47abacc2a84d56f3614d92fd62ed36ddde459664b9301dcd1d61781cfcc026bcb2399bee7e7"
  "5681a80b7bf500e2d08ceae1c42ec0b707927f2b2fe92ae852087d25f1d260cc74905ee5f9b2"
  "54ed05494a9fe06732c3680992dd6f0dc634568d11542a705f83ae96d2a49763d5fbb24398ed"
  "f3702bc94bc168190166492b8671de874bb9cecb058c6c8344aa8c93754d6effcd44a41ed7de"
  "0a9dcd9144437f212b18881d042d331a4618a9e630ef9bb66305e4fdf8f0391b3b2313fe549f"
  "0189ff968b92f33c266a4bc2cffc897d1937eeb9e406f5d0eaa7a14782e76af3fce98f54ed23"
  "7b4a04a4159a5f6250a296a902880204e61d891c4da29f2d65f34cbb");

const auto rsa_pub_key_e = convert_from_hex("49d2a1");

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

TEST(crypto_sig_validation, rsa_2k_sha256_good) {
    auto key = crypto::key::load_rsa_public_key(rsa_pub_key_n, rsa_pub_key_e);

    ASSERT_TRUE(crypto::verify_signature(
      crypto::digest_type::SHA256, key, sig_good_msg, sig_good_sig));
}

TEST(crypto_sig_validation, rsa_2k_sha256_bad) {
    auto key = crypto::key::load_rsa_public_key(rsa_pub_key_n, rsa_pub_key_e);

    ASSERT_FALSE(crypto::verify_signature(
      crypto::digest_type::SHA256,
      key,
      bytes_view_to_string_view(sig_bad_msg),
      bytes_view_to_string_view(sig_bad_sig)));
}

TEST(crypto_sig_validation, rsa_2k_sha256_good_multi_string) {
    auto key = crypto::key::load_rsa_public_key(rsa_pub_key_n, rsa_pub_key_e);

    crypto::verify_ctx ctx(crypto::digest_type::SHA256, key);
    ctx.update(bytes_view_to_string_view(sig_good_msg));
    ASSERT_TRUE(std::move(ctx).final(bytes_view_to_string_view(sig_good_sig)));
}
