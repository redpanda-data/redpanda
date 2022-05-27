#
# Dependencies that have customizations or are not provided by system packages
# on supported platforms. These are compiled from source for production release
# builds and for public builds when a system package is not available.
#
fmt = subproject('fmt')
fmt = fmt.get_variable('fmt_dep')
seastar = dependency('seastar', include_type: 'system')
seastar_testing = dependency('seastar_testing', include_type: 'system')
seastar_perf_testing = dependency('seastar_perf_testing', include_type: 'system')
seastar_json2code = find_program('seastar-json2code')
crc32c = dependency('crc32c')
rapidjson = dependency('rapidjson')
ctre = dependency('ctre')
hdr_histogram = dependency('hdr_histogram',
  default_options: ['build_tests=false'])
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
krb5 = dependency('krb5')
krb5_gssapi = dependency('krb5-gssapi')
xml2 = dependency('libxml2')

# the re2 pkg-config settings in fc37 contain an erroneous c++11 flag which
# overrides our request for c++20. but the cmake configuration is clean. but on
# ubuntu there is only pkg-config. so prefer cmake first, then pkg-config.
re2 = dependency('re2', required: false, method: 'cmake')
if not re2.found()
  re2 = dependency('re2')
endif

# this enables our flat includes like `#include "bytes/bytes.h"`
common = declare_dependency(
  include_directories: include_directories('.'))
add_project_dependencies(common, language: 'cpp')

# ignore warnings that are common in tests. these should be fixed
# so that we can eliminate the disabling of these warnings.
add_project_arguments(
  '-Wno-deprecated-experimental-coroutine',
  language: 'cpp')

# the timeout multiplier and the timeout settings per test need some tuning to
# account for debug vs release builds as well as desktop vs CI environments.
add_test_setup('redpanda',
  exe_wrapper: [test_runner, '--meson', '--default_cores', '4'],
  is_default: true,
  timeout_multiplier: get_option('buildtype').startswith('debug') ? 5 : 2)

subdir('test_utils')
subdir('bytes')
subdir('random')
subdir('hashing')
subdir('serde')
subdir('finjector')
subdir('compression')
subdir('model')
subdir('json')
subdir('utils')
subdir('ssx')
subdir('security')
subdir('config')
subdir('features')
subdir('reflection')

# some tests have circular or forward dependencies if added along with their
# normal containing directory. a common example are tests that depend on the
# application fixture that isn't added until the end with subdir('redpanda').
# setup these tests here after all dependencies have been introduced above.
subdir('bytes/tests')
subdir('security/tests')
