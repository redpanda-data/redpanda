# Pandaproxy tests

Tests for Pandaproxy

## Running

```sh
vtools build pkg --clang --format tar
cp /path/to/pandaproxy.tar.gz pandaproxy.tar.gz
bamboo session run-test --sname <session> --path .
```
