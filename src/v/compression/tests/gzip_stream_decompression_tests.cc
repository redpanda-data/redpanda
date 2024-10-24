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

#include "bytes/iobuf.h"
#include "bytes/iostream.h"
#include "compression/gzip_stream_decompression.h"
#include "compression/internal/gzip_compressor.h"
#include "test_utils/randoms.h"
#include "utils/base64.h"

#include <seastar/core/iostream.hh>

#include <gmock/gmock.h>

using namespace compression;

namespace {

ss::input_stream<char> make_compressed_stream(ss::sstring s) {
    return make_iobuf_input_stream(
      internal::gzip_compressor::compress(iobuf::from(s)));
}

ss::sstring consume(gzip_stream_decompressor& gsd) {
    ss::sstring output;
    while (true) {
        auto buffer = gsd.next().get();
        if (!buffer) {
            break;
        }
        for (const auto& frag : *buffer) {
            output.append(frag.get(), frag.size());
        }
    }
    return output;
}

} // namespace

TEST(GzStrm, SmallChunkSize) {
    auto test_string = tests::random_named_string<ss::sstring>(100000);
    gzip_stream_decompressor gsd{make_compressed_stream(test_string), 5};
    gsd.reset();
    auto output = consume(gsd);
    ASSERT_TRUE(gsd.eof());
    ASSERT_EQ(output, test_string);
    gsd.stop().get();
}

TEST(GzStrm, SmallPayloadLargeChunkSize) {
    ss::sstring test_string("xyz");
    gzip_stream_decompressor gsd{make_compressed_stream(test_string), 4096};
    gsd.reset();
    auto output = consume(gsd);
    ASSERT_TRUE(gsd.eof());
    ASSERT_EQ(output, test_string);
    gsd.stop().get();
}

TEST(GzStrm, EmptyString) {
    ss::sstring test_string;
    gzip_stream_decompressor gsd{make_compressed_stream(test_string), 4096};
    gsd.reset();
    auto output = consume(gsd);
    ASSERT_TRUE(gsd.eof());
    ASSERT_EQ(output, test_string);
    gsd.stop().get();
}

TEST(GzStrm, NonStandardCompressionMethod) {
    // Tests payload compressed with gzip -9
    const auto payload
      = "H4sICG5pgmYCA21vYnktZGlja2FoAFVXwY7kthG9N9D/"
        "wL3ZgNxAznNYbMZJdmBnbXgGWeRkUFJJoociFZJqWXvaj8ghAZKf2y/"
        "Je0X17AReeFoUWax69epV6X6yS5FkYjAfJ+tdGI0L5hfXZmNDfz4Z85TW7jn/"
        "ejmf+PTl839+"
        "CtgtJnadzQ7nHky2mylbNHEwZZIsZo4hw2o232wwKvlbs6TY2tbvZsYzDcG6GYRPjck+"
        "bniVNzfP8KBR+3agX7BnIv6XGrO5MsE3mMtYtsFY2skFm798/"
        "jfXUtwOH0yeYsLyf803T5KSNd+"
        "LN39eZYzfNowhXmF7m1w36e5WpJtorSQRI78XCb30xpVs2mRDh6AuNPbl8z9//"
        "d6mzQW98W9xt6PwSms+2LImAJjLK6i+fP7XI6KAq96/"
        "wRGY7rx1M2zz2tkWRG+zWRfgiPOB+E8um0ls3xjGYTf1Cz972K5+/Wa3XG/"
        "1NsGBx0XSrPkT0/mIBJSoR2jmQIRW2mhLQ5wEF+tdriisyHkuNhTTSy7Id0Fe7xDsa/"
        "8bM8Rk9rgmmvLuKpkh3WDB7akwClxbPfnBeS/pFRqPEUAbgClpb8y8w/"
        "s+N0hoUbP0NgHxIEzOYB2u/Dg5WNIURa90xDVLRMqxBTjBW/"
        "fMQLhFufbm5tAHxAPmwvhjDGN148bgqdEDySZkz/"
        "fVYbjkevIJWJKdI4N4CJqP2AkIN8UZtyAsxEFT78zoiBpYObtxArYbmCr1ge6luvoO1n6g"
        "lwc527iGnjxWAou9vAIRd772t/53//7dz09/+sX84WJ+jJE1kvX9PfJi4NNDnmYr/"
        "oKjeNqBI8p3jDBZwcSJHt5vxsM0alE6l8XveD/"
        "ZK6ydT96VgquR4RBZvrJrVLtZ1pRJUhgIkQWI80iT61ZwjzxzAQwBbeDH+"
        "QQCaOU1UIUyxZU4PJgtrgA5I6PGIvZC4tb7aDZLzfAGUqZdrcNQRWqLyfcX86BoWmzZYQ4"
        "+a9H1yV0rqkfNL14kVN1KMsLBcqNG51KnzzFckG2psDyYgcDMO8AYzAj54P4xubn6eT5pl"
        "eLXdMfU1lPucKa389LQiU+fIF4fIClzy9cKW0bIX8+cT/9/"
        "lQvX6NdQbHKeEa+"
        "ZF7eCCoOviAdGNpATCGbJTQ2pTdjEfetS2Ys0EwZesJthDQL9QUyzSLlTZCUvyDQ4sr/"
        "yBM5N+xKzGVEaeYUCgtnrAgkBsEHlYhbWhy0MNck/"
        "Vof8ImAUG+"
        "kzR9xzPi1wp3OLV7EBpa4SyAEzpDhDR7wDGMgodb3IstTOcuhSps6WI65ZwJTedernc4id"
        "FvQiEbZVZuEJ5W4AXXE4kFu261BD6uCEGjMFmgo/"
        "zicGhUtQVNTVDKXg3wfTWST+iaWMf8zP2ubiylpEZW2BtkavoLXwA8X1kbJozQIBijniD/"
        "wzg4dOuTyZe6uRgDFwz82aVBVx3pDB2v7OPJxPgE4Koir2+UWT8+"
        "QWukKhwOZbUeU1AdF8NGCsZdBeeb0j9aiZ5yAb4gVofo4ot1r6oe4Wl4D5CFTRTakBCggC"
        "q73zfILqquPKlQDm0CvtL0yZCDs/WmgE6/pcu65KnnaHWVRtqs8BMgK/"
        "VbHRNlQGOlf2m7j91QYkrETythVf0K8S9c60CAQSna4kE3ISeognTEEI8Q60J30RwZCR6C"
        "7Os6ROiIsezy+"
        "9aiKLsTxcMKdQYJg1L0NpXnErV8zhZdUVxkUdyezuSahVfdzAx40uVFWG19qZqohrAYTYe"
        "iqA11xtNk/"
        "CQPDr+lKXXcRrXW1x8yeu18nCAtgNsgsF1QJxqGaz0TY1EHBldT8OkF9YUm1/"
        "NrhVFQvc6rXLq//"
        "fjfYTByqms2bjnpI227mlrsmhcjUN0CaIA1j+"
        "aFtENdVZKkSK319irdB7KCv7xPn0nreCnPcRFeyQjEfvlqr4upM114FXCBDtuMikw0CIqU"
        "wVVXStQjgVbaj5W+"
        "TvZ1AUoHiHJGT0cNRqpvkgPit1bSUF3WYSQNuiaLJrZMt816Ho5ZG6hDttZf3gfueAFg6a"
        "Un4SfK8dEHhikaVkR8vR5mgP8CPf1frIHIH62x0CAQDGHJeODTABWLS7XI8xFEAj3mf1hE"
        "WcDyBRvvawqlpkfRzKUZZoRG4cdaYF5d2gA4s2LQBOraKq1oGC5KNbxBTqJxCJP66lDtTn"
        "EycVwkaiZABwp8wQeUYb2kEvAoy+"
        "wPlYEw5oF285hFM1HQfOSKqu7NZgaEAz1kXcrPNtg8nR8ZcuYg58BpjvUetMfy0Rl9+"
        "ad0lqV6TWMA3ikZsRA8Nb5YHpVeV2Vqm8JU8ZBLF8o0so85kFlW4Mb9D+OhXAkqzWA/"
        "X4ZR44agys4tQD0dLZSfeA5RhCkXEcDKO8QQuuUqoTWhcRaVD8ZtNWIM+no/"
        "jBCO9mV26aRVTv4CXYnbTDYsxOh1rbHoOq6MCxR65DxV5ac70LGg6dQtfu4M6HqOqOL50V"
        "1zDHv/EH0h+0U90iQ05zhQrNODt+G3WH4FIeBmS7NoOLzo/"
        "lECXZa6UgrTP5rPLBILEA1o8ri+AhMCLqhdaqnlLglbF4J/"
        "p5x7ey5+ammXCJi+jksMLhkTXeGAGP2FZWPnDHBgAv5u8I7atTJOcaACCiF+/"
        "Z1jEcoQscXzpjEAyN5upSWeWGO8b4vsZQ9SvOi825LlgaARZZjmqDPCfbHRlFpomIMuwny"
        "JNSClzAgEgVYrnUCjTK+bTf8flrkfpj1PHoEgDsCX8Q/"
        "dFZA2cyVBEtYQixtwEYhGJx8OsU3OlsouTorl77iH6S9vopUXsSu4S+r9mDgmKiQLu4+"
        "Ubc7fxqGgBOAMmF88kB4R+"
        "lKlf1qqWAfsdRXo4pLfB7asEgOVYt5PDRQzmQHk7AVRKRRyVMbWiz5de77vQy5uZ/"
        "Lj+PWgAQAAA=";
    const auto decoded = base64_to_string(payload);
    iobuf b;
    b.append(decoded.data(), decoded.size());
    gzip_stream_decompressor gsd{make_iobuf_input_stream(std::move(b)), 4096};
    gsd.reset();
    auto output = consume(gsd);
    ASSERT_TRUE(gsd.eof());
    ASSERT_THAT(output, testing::HasSubstr("Call me Ishmael."));
    gsd.stop().get();
}
