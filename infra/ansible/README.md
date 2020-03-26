## Usage

```bash
vtools deploy ansible \
  --playbook <path/to/playbook.yml> \
  --playbook <another.yml> \
  --var a_var=foo \
  --var another_var=bar
```

The order in which playbooks are executed is the same order in which 
flags are given (first `playbook.yml` then `another.yml` in the 
example above). Path to playbooks can be a relative path.

## Examples

Deploy and initialize RP on i3.large (one NVMe SSD):

```bash
vtools deploy cluster nodes=3
vtools deploy ansible \
  --playbook infra/ansible/playbooks/provision-test-node.yml \
  --playbook infra/ansible/playbooks/redpanda-start.yml \
  --var rp_pkg=build/release/gcc/dist/rpm/RPMS/x86_64/redpanda-0.0-dev.x86_64.rpm
```

Deploy and initialize RP on m5ad.4xlarge (two NVMe SSDs, RAID0):

```bash
vtools deploy cluster instance_type=m5ad.4xlarge nodes=3
vtools deploy ansible \
  --playbook infra/ansible/playbooks/provision-test-node.yml \
  --playbook infra/ansible/playbooks/redpanda-start.yml \
  --var rp_pkg=build/release/gcc/dist/rpm/RPMS/x86_64/redpanda-0.0-dev.x86_64.rpm
  --var with_raid=true
```

Deploy but do not start RP on i3.large instances (one NVMe SSD), then 
run ducktape tests:

```bash
vtools deploy cluster nodes=7
vtools deploy ansible \
  --playbook infra/ansible/playbooks/provision-test-node.yml \
  --var rp_pkg=build/release/gcc/dist/rpm/RPMS/x86_64/redpanda-0.0-dev.x86_64.rpm
vtools test pz --use-existing-cluster
```
