libsecurity = library('security',
  'scram_algorithm.cc',
  'scram_authenticator.cc',
  'gssapi_authenticator.cc',
  'gssapi_principal_mapper.cc',
  'credential.cc',
  'acl.cc',
  'mtls.cc',
  'license.cc',
  'krb5.cc',
  'krb5_configurator.cc',
  implicit_include_directories: false,
  dependencies: [seastar, ctre, bytes, utils, model, json, cryptopp, random, re2, krb5, krb5_gssapi])

security = declare_dependency(
  dependencies: [seastar, ctre, bytes, utils, model, json, cryptopp, random, re2, krb5, krb5_gssapi],
  link_with: libsecurity)
