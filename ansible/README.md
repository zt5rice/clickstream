# ansible — Provisioning Playbooks

Status: scaffold (TODO) · Purpose: config management / provisioning to complement Terraform
(hits "Terraform, Ansible" lines in JDs, e.g., Sony  / ).

## Planned playbooks

- `bootstrap-demo.yml` — install Docker dependencies on demo nodes (local VMs or EC2)
- `eks-bastion.yml` — prepare an EKS bastion host (aws cli, kubectl, helm, eksctl)

## TODO

- [ ] inventory + ansible.cfg
- [ ] bootstrap-demo.yml (apt/packages, docker, python)
- [ ] eks-bastion.yml (aws cli, kubectl, helm)
- [ ] Lint with ansible-lint
