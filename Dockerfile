# syntax=docker/dockerfile:1

# Multi-arch image (linux/amd64, linux/arm64):
#
#   docker buildx build --platform linux/amd64,linux/arm64 -t <registry>/pingap-k8s:<version> .
#
# The builder always runs on the build host's platform and cross-compiles for the
# target, so no emulation is needed. The runtime stage only copies the binary.

# Toolchain and Debian release are pinned together with the runtime stage below
# (trixie), so the binary runs on the glibc it was linked against. Both base
# images are pinned by the digest of their multi-arch index as well as by tag;
# Dependabot (.github/dependabot.yml) proposes updates.
FROM --platform=$BUILDPLATFORM rust:1.99.1-trixie@sha256:931c2c0531a8b143e044f0f3914dfe802f15bb3d1955f0b657e9359327ac2ffc AS builder

ARG TARGETARCH
ARG BUILDARCH

# protobuf-compiler + libprotobuf-dev (well-known .proto files): etcd-client
# generates its gRPC code at build time.
# cmake + clang: native code of the TLS crypto provider.
# gcc-<arch> + libc6-dev-<arch>-cross: only when cross-compiling.
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) target=x86_64-unknown-linux-gnu;  gnu=x86-64-linux-gnu;  deb=amd64 ;; \
      arm64) target=aarch64-unknown-linux-gnu; gnu=aarch64-linux-gnu; deb=arm64 ;; \
      *) echo "unsupported TARGETARCH: ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    pkgs="protobuf-compiler libprotobuf-dev cmake clang"; \
    if [ "${TARGETARCH}" != "${BUILDARCH}" ]; then pkgs="${pkgs} gcc-${gnu} libc6-dev-${deb}-cross"; fi; \
    apt-get update; \
    apt-get install -y --no-install-recommends ${pkgs}; \
    rm -rf /var/lib/apt/lists/*; \
    rustup target add "${target}"; \
    echo "${target}" > /rust-target

# Linkers and C compilers per target. Debian provides the triplet-prefixed
# gcc for the native architecture too, so these are valid in both cases.
ENV CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc \
    CC_x86_64_unknown_linux_gnu=x86_64-linux-gnu-gcc \
    CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=aarch64-linux-gnu-gcc \
    CC_aarch64_unknown_linux_gnu=aarch64-linux-gnu-gcc \
    CARGO_TERM_COLOR=never

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,id=pingap-k8s-target-${TARGETARCH},sharing=locked \
    set -eux; \
    target="$(cat /rust-target)"; \
    cargo build --release --locked --bin pingap-k8s --target "${target}"; \
    cp "target/${target}/release/pingap-k8s" /pingap-k8s

# Runtime: distroless (glibc, CA certificates, no shell, no package manager),
# running as the unprivileged "nonroot" user (65532).
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:3ee00291cf386ec52a552e6a858d555debd74776eb4bfa53fa3df2cd6677515e

LABEL org.opencontainers.image.title="pingap-k8s" \
      org.opencontainers.image.description="Kubernetes controller for Pingap: syncs Service endpoints into Pingap's etcd config" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.source="https://github.com/mehdyhaghy/pingap-k8s"

COPY --from=builder /pingap-k8s /usr/local/bin/pingap-k8s

USER 65532:65532
EXPOSE 9090
ENTRYPOINT ["/usr/local/bin/pingap-k8s"]
CMD ["--config", "/etc/pingap-k8s/config/config.toml"]
