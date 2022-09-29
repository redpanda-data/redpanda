
# features

## Why does this file get its own top-level library?

The feature table may be accessed from many different parts
of redpanda, and is instantiated in application.cc very early
in application startup.

