The `random` library is a foundational library that provides access to random
data generation utilities for primitive types such as integers and characters.

## Main Entry Points

**For non-cryptographically secure random numbers**, use `random_generators`:
- Fast, high-quality pseudorandom number generation using PCG64
- Suitable for simulations, testing, sampling, and general-purpose randomness
- Configurable seeding modes for deterministic testing vs random production behavior
- Thread-safe global instance available via `random_generators::global()`
- Provides common distributions like `get_int()` for uniform integer generation
- For more specialized distributions (normal, exponential, etc.), use standard
  library `<random>` distributions or Abseil distributions with the underlying
  engine accessible via `rng::engine()`
- For details on seeding behavior see [random values in tests](https://redpandadata.atlassian.net/wiki/spaces/CORE/pages/1406271495/Random+values+in+tests)

**For cryptographically secure random numbers**, use `secure_random`:
- Cryptographically secure random number generation
- Suitable for security-sensitive applications like key generation, nonces, etc.
- Uses system entropy sources

## Usage Guidelines

Modules that want to provide random generation for higher-order types should
link against the `random` library and provide specialized generators using
the appropriate underlying primitives based on their security requirements.
