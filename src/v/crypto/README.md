# Redpanda Crypto Library

The Redpanda crypto library provides a convenient API for any subsystem that
desires to use a cryptographic function.  It will abstract away the actual
implementation (OpenSSL).

This library provides the following implementations:

* RSA PKCSv1.5 signature verification
* HMAC
* MD5, SHA256 and SHA512 digest generation
* Cryptographically secure DRBG
* Base64 decoding

## Cryptographic Secure Digest

This library currently supports the following digest types:

* MD5
* SHA256
* SHA512

There are two ways of performing a digest: one-shot and multi-part.

For one-shot, you can do the following:

```c++
#include "crypto/crypto.h"

bytes msg = {...};
auto digest = crypto::digest(crypto::digest_type::SHA256, msg);
```

For multi-part operation, you can do the following:

```c++
#include "crypto/crypto.h"

bytes msg = {...};
crypto::digest_ctx ctx(crypto::digest_type::SHA256);
ctx.update(msg);
auto digest = std::move(ctx).final();
```

## HMAC

Like digests, this library can perform HMAC two ways: one-shot and multi-part.

For one-shot:

```c++
#include "crypto/crypto.h"

bytes key = {...};
bytes msg = {...};

auto sig = crypto::hmac(crypto::digest_type::SHA256, key, msg);
```

For multi-part:

```c++
#include "crypto/crypto.h"

bytes key = {...};
bytes msg = {...};

crypto::hmac_ctx ctx(crypto::digest_type::SHA256, key);
ctx.update(msg);
auto sig = std::move(ctx).final();
```

## Asymmetric Key Handling

Currently, this library only supports RSA keys.  There are two ways of loading a
key from a buffer.

First, if the key is held within a buffer in PKCS8 format, you can use
`crypto::load_key`:

```c++
#include "crypto/crypto.h"

bytes rsa_priv_key {...};

auto key = crypto::key::load_key(
  rsa_priv_key,
  crypto::format_type::PEM,
  crypto::is_private_key_t::yes);
```

The above function can determine the type of key held in the buffer but the
caller is responsible for indicating the format the key is in (PEM or DER) and
whether or not it's the public half of the key or the private key.

The other way of loading a key is by its parts.  Currently only RSA public key
loading is available:

```c++
#include "crypto/crypto.h"
bytes modulus {...};
bytes public_exponent {...};

auto key = crypto::key::load_rsa_public_key(modulus, public_exponent);
```

## Signature Verification

Performing signature verification can be done either one-shot or multi part.

One-shot:

```c++
#include "crypto/crypto.h"

auto key = {...};
bytes msg {...};
bytes sig {...};

auto sig_verified = crypto::verify_signature(
  crypto::digest_type::SHA256,
  key,
  msg,
  sig
);
```

For multi-part:

```c++
#include "crypto/crypto.h"

auto key = {...};
bytes msg {...};
bytes sig {...};

crypto::verify_ctx ctx(crypto::digest_type::SHA256, key);
ctx.update(msg);
bool verified = std::move(ctx).final(sig);
```

## RNG Generation

To get cryptographically secure random data, please use
`crypto::generate_random`.  There are two functions you can use.  One will
place random data into the provided buffer and another will return an allocated
buffer with random data.  Both functions also have a flag to indicate if the caller
wants to use the 'private' DRBG.  For more information on this please refer to
OpenSSL's [documentation](https://www.openssl.org/docs/man3.0/man3/RAND_priv_bytes.html)
on this.
