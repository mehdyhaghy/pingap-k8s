# Security policy

## Reporting a vulnerability

Please do not open a public issue. Report it privately through GitHub: the repository's
**Security** tab, then **Report a vulnerability**. Include the version (`pingap-k8s --version`),
how to reproduce it, and the impact you see.

You get an answer within 7 days. A fix is released as a new patch version (see
[Releasing](README.md#releasing)) and credited in the CHANGELOG unless you ask otherwise.

## Supported versions

Only the latest release gets fixes.

## Scope

The controller, the Helm chart, the plain manifests, the kubectl plugin and the release
pipeline. Vulnerabilities in Pingap itself belong to the
[Pingap project](https://github.com/vicanso/pingap). The README's [Security](README.md#security)
section describes the threat model and the hardening defaults.
