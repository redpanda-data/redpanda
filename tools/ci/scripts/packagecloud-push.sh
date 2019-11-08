#!/bin/bash
set -e

if [ -z "$TAG_NAME" ]; then
  echo "No TAG_NAME variable defined, skipping."
  exit 0
fi

if [[ "$TAG_NAME" != *"release"* ]]; then
  echo "Branch $TAG_NAME is not a release branch, skipping."
  exit 0
fi

package_cloud push vectorizedio/v/ubuntu/xenial build/dist/debian/*.deb
package_cloud push vectorizedio/v/el/7/ build/dist/rpm/RPMS/x86_64/*.rpm
