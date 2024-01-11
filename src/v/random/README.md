The `random` library is a foundational library that provides access to random
data generation utilities for primitive types such as integers and characters.

Modules that want to provide random generation for high order types should
link against the `random` library and provide specialized generators.
