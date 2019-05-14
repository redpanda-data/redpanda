# How to get started [![build](https://api.shippable.com/projects/5cab6e979bbe6e0007384c8b/badge?branch=master)]()
## Building `redpanda`/C++
### Debug
Assuming from the root of the repo:
```sh
./tools/build.py --deps true --targets cpp --build debug
```

### Release
```sh
./tools/build.py --deps true --targets cpp --build release
```
That's it!

### Using internal LLVM toolchain
```sh
./tools/build.py --deps true --targets cpp --build release --clang internal
```

### Incremental CPP builds
To do incremental builds, just `cd $ROOT/build/Release && ninja <target name>`
for example: `cd build/Release && ninja redpanda` will build the
 `redpanda` binary & transitive deps only

## Building Go code (`rpk`, ..., etc.)

All Go binaries can be found in `build/go/bin`

```sh
./tools/build.py --deps true --targets go --build release
```

## Creating packages

Currently `tar.gz`, `deb` and `rpm` packages are supported.
(Assuming that the code was built before)

```sh
    ./tools/build.py --packages rpm deb tar
```

`--packages` flags is a list of packages to create.
Packages can be found in `<root>/build/dist/<package_type>`

## Contributing
Also, before submitting a patch:
* Please squash all your commits into 1 change
* Run: `./tools/build.py --build=release --fmt=all --log=DEBUG`
