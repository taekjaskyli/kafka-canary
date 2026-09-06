# Contributing to kafka-canary

Thanks for wanting to help.

## Licensing of contributions

kafka-canary is Apache License 2.0. Opening a pull request licenses your
changes under Apache-2.0.

## Making a change

- **Branch from `main`.** Pull requests target `main`.
- **Go 1.22.2.** Set `GOTOOLCHAIN=local` (the Makefile does this) so module
  commands do not upgrade the toolchain.
- **Tests come with the change.** Unit: `make test`. End-to-end needs Docker
  Compose v2: `make test_e2e` (starts a KRaft broker from
  `test/compose-kafka.yaml` on host port 9092). CI runs both.
- **Run the checks before pushing** — `make test`, `make test_e2e`, `make go_build`,
  `make helm_lint`. CI runs them anyway; running them locally is faster.

```shell
make test
make go_build
make docker_build
make helm_lint
```

## Reporting bugs and asking questions

- Bugs and feature requests: [GitHub issues](https://github.com/taekjaskyli/kafka-canary/issues).
- Security problems: do **not** open a public issue. Use
  [private vulnerability reporting](https://github.com/taekjaskyli/kafka-canary/security/advisories/new)
  (see [SECURITY.md](./SECURITY.md)).

## Releasing

The [Release](.github/workflows/release.yml) workflow runs on the tag: multi-arch
image to GHCR, Helm chart to OCI and `gh-pages`.

1. Update `CHANGELOG.md` (that section becomes the GitHub Release notes) and Helm `Chart.yaml` (`version` / `appVersion`).
2. Merge to `main`.
3. `git tag 0.8.0 && git push origin 0.8.0`

One-time GitHub setting after the first release: **Settings → Pages → Source:
Deploy from a branch → `gh-pages` / `/`**.

## Maintainer

[@taekjaskyli](https://github.com/taekjaskyli)
