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
