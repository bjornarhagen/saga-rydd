FROM public.ecr.aws/docker/library/golang:1.27.1-bookworm@sha256:648f440f42a0958804efb24df176f806f9d353b41f1c0627f666428e40310f6b

# Seed writable caches for arbitrary host UID/GID, including on native Linux.
RUN mkdir -p /cache/build /cache/modules /tmp/rydd-home /run/rydd \
    && chmod 1777 /cache /cache/build /cache/modules /tmp/rydd-home /run/rydd

ENV GOCACHE=/cache/build \
    GOMODCACHE=/cache/modules \
    GOTOOLCHAIN=local \
    GOFLAGS=-mod=readonly \
    HOME=/tmp/rydd-home

WORKDIR /workspace
CMD ["./scripts/tasks", "check"]
