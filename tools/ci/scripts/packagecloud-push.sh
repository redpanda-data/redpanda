#!/bin/bash
set -e

source tools/base_script.sh

if [ -z "$BUILD_TYPE" ]; then
  fatal "No BUILD_TYPE variable defined."
fi

if [ -z "$COMPILER" ]; then
  fatal "No COMPILER variable defined."
fi

if [ "$COMPILER" != "clang" ] || [ "$BUILD_TYPE" != "release" ]; then
  log "we only upload clang-release builds to packagecloud, skipping."
  exit 0
fi

if [ -z "$TAG_NAME" ]; then
  log "No TAG_NAME variable defined, skipping."
  exit 0
fi

if [[ "$TAG_NAME" != *"release"* ]]; then
  log "Branch $TAG_NAME is not a release branch, skipping."
  exit 0
fi

deb_distros=(
  "debian/jessie"
  "debian/stretch"
  "debian/buster"
  "debian/bullseye"
  "ubuntu/trusty"
  "ubuntu/utopic"
  "ubuntu/vivid"
  "ubuntu/wily"
  "ubuntu/xenial"
  "ubuntu/yakkety"
  "ubuntu/zesty"
  "ubuntu/artful"
  "ubuntu/bionic"
  "ubuntu/cosmic"
  "ubuntu/disco"
  "ubuntu/eoan"
)

rpm_distros=(
  "el/6"
  "el/7"
  "el/8"
  "fedora/30"
  "fedora/31"
  "ol/7"
)

pids=()
for d in ${deb_distros[*]}; do
  package_cloud push --skip-errors vectorizedio/v/$d build/$BUILD_TYPE/dist/debian/*.deb &
  pids+=("$!")
done
for d in ${rpm_distros[*]}; do
  package_cloud push --skip-errors vectorizedio/v/$d build/$BUILD_TYPE/dist/rpm/RPMS/x86_64/*.rpm &
  pids+=("$!")
done
for p in ${pids[*]}; do
  wait $p
done
