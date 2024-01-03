The base library is a foundational library. The rough guidelines for determining
if something belongs in the base library are:

1. It would be reasonable if every target depended on it (e.g. vlog.h).
2. It contains no dependencies on other Redpanda targets (e.g. utils).
3. Its external dependencies may also be considered foundational (e.g. seastar).
