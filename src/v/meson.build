#
# makes the sub-subprojects available to meson
# then use -Dredpanda_build=true --wrap-mode=forcefallback
#
if get_option('redpanda_build')
  redpanda_subprojects = subproject('redpanda-subprojects')
endif

#
# Dependencies that have customizations or are not provided by system packages
# on supported platforms. These are compiled from source for production release
# builds and for public builds when a system package is not available.
#
seastar = dependency('seastar', include_type: 'system')
seastar_testing = dependency('seastar_testing', include_type: 'system')
seastar_perf_testing = dependency('seastar_perf_testing', include_type: 'system')
seastar_json2code = find_program('seastar-json2code')
crc32c = dependency('crc32c')
rapidjson = dependency('rapidjson')
ctre = dependency('ctre')
hdr_histogram = dependency('hdr_histogram')
base64 = dependency('base64')
avro = dependency('avro')

cpp = meson.get_compiler('cpp')

roaring = dependency('roaring', required: false)
if not roaring.found() or not cpp.compiles('''
    #include <roaring/roaring.hh>
    int main(int, char**) {
      roaring::Roaring r;
      return 0;
    }''',
    dependencies: [roaring])
 roaring = subproject('roaring')
 roaring = roaring.get_variable('roaring_dep')
endif

#
# Dependencies that are satisified by system packages for the public build, but
# are compiled from source for production release builds.
#
gnutls = dependency('gnutls')
xxhash = dependency('libxxhash')
absl = dependency('absl',
  modules: [
    'absl::flat_hash_map',
    'absl::btree',
    'absl::flat_hash_set',
    'absl::node_hash_set',
    'absl::node_hash_map',
    'absl::random_seed_sequences'])
yaml = dependency('yaml-cpp')
zlib = dependency('zlib')
zstd = dependency('libzstd')
snappy = dependency('snappy')
boost = dependency('boost',
  modules: [
    'program_options',
    'thread',
    'filesystem',
    'unit_test_framework'])
protobuf = dependency('protobuf',
  modules: ['protobuf::libprotobuf', 'protobuf::libprotoc'])
cryptopp = dependency('libcrypto++', 'cryptopp')

# this enables our flat includes like `#include "bytes/bytes.h"`
common = declare_dependency(
  include_directories: include_directories('.'))
add_project_dependencies(common, language: 'cpp')

# ignore warnings that are common in tests. these should be fixed
# so that we can eliminate the disabling of these warnings.
add_project_arguments(
  '-Wno-deprecated-experimental-coroutine',
  language: 'cpp')

subdir('test_utils')
subdir('ssx')
subdir('bytes')
subdir('random')
subdir('hashing')
subdir('serde')
subdir('finjector')
subdir('compression')
