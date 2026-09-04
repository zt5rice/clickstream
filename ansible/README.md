# ansible — provisioning playbooks (P2-07)

## Files

- `inventory.yml` — localhost (connection: local).
- `playbooks/provision-local-demo.yml` — **runnable locally**: verifies the
  Docker/colima runtime and ensures the core local services are up, then writes
  a small report to `/tmp/clickstream-ansible-provision.txt`.
- `playbooks/prepare-eks-bastion.yml` — reference playbook for a Linux EKS
  bastion / demo node (Debian/RedHat). The `eks_bastion` group is intentionally
  **not** in the local inventory, so it cannot run against localhost by
  accident; run it against real hosts with an SSH inventory.

## Usage

```bash
make ansible-check     # run the local-demo playbook in --check mode
make ansible-provision # actually ensure the local services are running
```

## Honesty note

The local playbook is run and verified on the developer machine. The EKS
bastion playbook is written but has not been executed against a real cloud
host — it is reference material for the Phase 3 deployment track.
