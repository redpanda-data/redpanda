# How to get started

## Building Red Panda

## Debug

Assuming from the root of the repo:

```sh
./tools/build.py --deps=true --targets=cpp --build=debug
```

### Release

```sh
./tools/build.py --deps=true --targets=cpp --build=release
```

That's it!

## Incremental CPP builds

To do incremental builds, just `cd $ROOT/build/Release && ninja <target name>`
for example: `cd build/Release && ninja redpanda` will build the
 `redpanda` binary & transitive deps only


## Building golang code (RPK, ..., etc.)

```sh
./tools/build.py --deps=true --targets=go --build=release
```

All Go binaries can be found in `build/go/bin`

## Contributing

Also, before submitting a patch:

* Please squash all your commits into 1 change
* Run: `./tools/build.py --build=release --fmt=all --log=DEBUG
