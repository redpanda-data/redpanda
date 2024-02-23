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

