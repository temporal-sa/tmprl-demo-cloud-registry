# Instruqt course example

This directory shows how to add the secret-broker CLI to an Instruqt course
image and use it during the track setup lifecycle.

## 1. Add the CLI to the course image

Use the public artifact image as a named build stage, as shown in
[Dockerfile](./Dockerfile). It contains only the static `/secret-broker` binary
and is not intended to be the final course image.

The example defaults to the moving `main` tag so it can be built as-is. For a
real course, pass an immutable `main-YYYYMMDDHHMMSS-<git-sha>` release when
building the course image:

```bash
docker build \
  --build-arg SECRET_BROKER_VERSION='main-YYYYMMDDHHMMSS-<git-sha>' \
  --tag example-instruqt-course:latest \
  .
```

Docker selects the matching `amd64` or `arm64` artifact automatically. Replace
the example's `ubuntu:24.04` final stage with the existing base image and
contents of your course image.

## 2. Configure the Instruqt secret

Declare the broker credential in the course's `instruqt/config.yml`:

```yaml
secrets:
  - name: TEMPORAL_LITELLM_BROKER_SECRET
```

Configure its value in Instruqt. Do not pass the credential as a Docker build
argument or store it in the course image.

## 3. Mint a learner key during setup

Copy the relevant command from [setup.sh](./setup.sh) into the course's track
setup lifecycle script. Instruqt supplies the track and participant identity,
along with `TEMPORAL_LITELLM_BROKER_SECRET`, at runtime.

The example requests a one-day key with a maximum budget of 5. It does not
specify `--model`, so all models exposed by the LiteLLM gateway are allowed.
Budget and duration must always be supplied explicitly.

`secret-broker` writes the resulting values to `/root/.litellm-env`,
`/etc/environment`, and `/root/.bashrc`. Source `/root/.litellm-env` when later
commands in the same setup script need the minted values immediately.
