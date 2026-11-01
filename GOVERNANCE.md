# Governance

This document describes how pingap-k8s is run today and how that changes as more people join.

## Values

- Decisions are made in public: in issues and pull requests on GitHub.
- The project stays vendor-neutral. No company controls it, and maintainers act for the
  project, not for their employer.
- Users come first: compatibility, security and clear documentation outweigh new features.

## Roles

**Users** run the project and report problems or ideas in issues.

**Contributors** send pull requests, reviews, documentation, tests or answers to issues.
Anyone can contribute; see [CONTRIBUTING.md](CONTRIBUTING.md).

**Maintainers** are listed by GitHub handle in [`.github/CODEOWNERS`](.github/CODEOWNERS). They
review and merge pull requests, cut releases, triage issues and security reports, and decide the
[roadmap](ROADMAP.md).

## Decisions

Most decisions are made by lazy consensus in an issue or pull request: a proposal is accepted
when no maintainer objects within a reasonable time (at least 3 working days for changes to
behavior, APIs, the chart values or this document).

If consensus cannot be reached, the maintainers vote. A decision needs a simple majority of
all maintainers; changes to this document need two thirds. While there is a single maintainer,
that maintainer decides, and records the reasons in the issue or pull request.

Pull requests from a maintainer need the approval of another maintainer once there are at
least two.

## Becoming a maintainer

A contributor can become a maintainer after a sustained record of good contributions and reviews
(as a guide: several merged non-trivial pull requests over at least three months). Any
maintainer can nominate them in a pull request to `.github/CODEOWNERS`; it is accepted by lazy
consensus of the maintainers.

The project aims for maintainers from more than one organization. Once there are at least
three maintainers, no single organization may have a majority of them.

## Stepping down and removal

A maintainer can step down at any time by opening a pull request to `.github/CODEOWNERS`. A
maintainer inactive for six months, or one who breaks the [Code of Conduct](CODE_OF_CONDUCT.md),
can be removed by a two-thirds vote of the other maintainers.

## Security

Vulnerability reports follow [SECURITY.md](SECURITY.md) and are handled privately by the
maintainers.

## Changes

This document is changed by pull request, as described under Decisions.
