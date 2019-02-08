# How to get started

## Debug 

Assuming from the root of the repo:

```sh
./tools/build.py --deps=true --build=release
```

That's it!

To do incremental builds, just `cd $ROOT/build/Release && ninja <target name>`
for example: `cd build/Release && ninja redpanda` will build the 
`redpanda` binary & transitive deps only


## Release 

Also, before submitting a patch:

* Please squash all your commits into 1 change
* Run: `./tools/build.py --build=release --fmt=all --log=DEBUG`


