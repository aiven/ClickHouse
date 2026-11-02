# Agent hooks for Aiven Buildkite (ClickHouse)

Buildkite **repository** hooks under `.buildkite/hooks/` run only *after*
checkout. Root-owned `tests/integration/**/_instances*` left by a killed DinD
job can make the *next* checkout fail with exit 128 before any repo script
runs. That cleanup must live in an **agent** `pre-checkout` hook.

## Install on each expensive-* AMI / fleet image

From a machine that already has this branch checked out (or after copying the
file onto the image):

```bash
sudo install -m 0755 /path/to/ClickHouse/.buildkite/agent-hooks/pre-checkout \
  /etc/buildkite-agent/hooks/pre-checkout
sudo systemctl restart buildkite-agent   # if required by your unit setup
```

Confirm the agent `hooks-path` (default `/etc/buildkite-agent/hooks`) in
`/etc/buildkite-agent/buildkite-agent.cfg`.

## Keep in sync

The body of `pre-checkout` is intentionally duplicated from
`.buildkite/cleanup_checkout_pollution.sh` (the repo script is not available
before checkout). If you change one, change the other.

## Defense in depth (already in-repo)

- `.buildkite/hooks/pre-exit` — wipe after a normal/failed command exit
- `.buildkite/provision.sh` — start + `EXIT` trap cleanup once checkout succeeded
