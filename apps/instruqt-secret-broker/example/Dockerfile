# syntax=docker/dockerfile:1.7

# Use an immutable release for deployed courses. The moving main tag is kept as
# the default only so this example remains directly buildable.
ARG SECRET_BROKER_VERSION=main
FROM public.ecr.aws/s1u1b8l5/tmprl-dem-cld/instruqt-secret-broker:${SECRET_BROKER_VERSION} AS secret-broker

# Replace this stage with the existing base and contents of the course image.
FROM ubuntu:24.04

COPY --from=secret-broker /secret-broker /usr/local/bin/secret-broker

# This checks only that the binary is present and executable. The Instruqt
# credential is deliberately supplied later, when the setup lifecycle runs.
RUN secret-broker --version
