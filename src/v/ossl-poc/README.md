# OpenSSL In Seastar Proof of Concept

The purpose of this small application is to demonstrate how to use OpenSSL
within Seastar and to validate its performance.

## Pre-reqs

You will need a self-signed certificate and key.  For example you can run

```shell
openssl req -x509 -newkey rsa:4096 -keyout key.pem -out cert.pem -sha256 \
-days 3650 -nodes -subj "/C=XX/ST=StateName/L=CityName/O=CompanyName/OU=CompanySectionName/CN=CommonNameOrHostname"
```

to generate a self-signed cert and key.

You will also need to create an openssl.cnf file.  One can be found in this
directory in [openssl.cnf](openssl.cnf)

You will need to modify the include line to point to the `fipsmodule.cnf` file
that is installed in `vbuild/<build-type>/clang/rp_deps_install/openssl`.

## Building

The target is `ossl-poc` if you use `task rp:build` to build the project.

## Running

To run this program, you will need to set the environment variable `OPENSSL_CNF`
to point to the `openssl.cnf` file.  Then you need to provide the following
options:

- `--key` - path to the key file
- `--cert` - path to the certificate
- `--module-path` - path to the ossl-modules directory found in `vbuild/<build-type>/clang/rp_deps_install/lib64/ossl-modules`

Example:

```shell
OPENSSL_CNF=./src/v/ossl-poc/openssl.cnf ./vbuild/debug/clang/bin/ossl-poc \
-c 2 \
-m 4G \
--key <path to key>
--cert <path to cert>
--module-path ./vbuild/debug/clang/rp_deps_install/lib64/ossl-modules
```
