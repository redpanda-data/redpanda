#!/bin/env bash
set -ex

function locate_kernel_image() {
  local kernel_version=$(awk '{print $3}' </proc/version)
  local grub_images=$(ls /boot/vmlinuz*)
  local kernel_image=""
  for image in $grub_images; do
    if grep -Eq "$kernel_version" "$image"; then
      kernel_image="$image"
    fi
  done
  echo "${kernel_image}"
}

function enable_and_prompt() {
  grubby --update-kernel=ALL --args="systemd.unified_cgroup_hierarchy=0"
  echo "It has been detected that the kernel parameter systemd.unified_cgroup_hierarchy is enabled"
  echo "It has been automatically disabled for you however, you'll need to restart your system for this to take effect"
  echo "Restart, then try running this bootstrap script again."
}

# Grab the value of systemd.unified_cgroup_hierarchy
kernel_image=$(locate_kernel_image)
cgroup_hierarchy_cmd="grubby --info \"${kernel_image}\" | grep -o -P 'systemd.unified_cgroup_hierarchy.{0,2}'"

# The attribute could be missing entriely...
if ! eval "${cgroup_hierarchy_cmd}"; then
  enable_and_prompt
  exit 1
fi

# Or it could be set to a value other than 0...
cgroup_hierarchy_result=$(eval "${cgroup_hierarchy_cmd}")
cgroup_hierarchy_value=$(echo "${cgroup_hierarchy_result}" | cut -d '=' -f 2)
if [ "${cgroup_hierarchy_value}" != '0' ]; then
  enable_and_prompt
  exit 1
fi

echo "systemd.unified_cgroup_hierarchy kernel parameter detected as disabled, OK"
