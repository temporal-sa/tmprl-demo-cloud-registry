#!/bin/sh
set -eu

base_url="${SECRET_BROKER_BASE_URL:-https://tmprl-dem-cld-secret-broker-429214323166-us-west-1.s3.us-west-1.amazonaws.com/secret-broker}"
version="${SECRET_BROKER_VERSION:-main}"
install_dir="${SECRET_BROKER_INSTALL_DIR:-/usr/local/bin}"

case "$(uname -m)" in
  x86_64 | amd64)
    architecture="amd64"
    ;;
  aarch64 | arm64)
    architecture="arm64"
    ;;
  *)
    echo "Unsupported architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

binary="secret-broker-linux-${architecture}"
temporary_directory="$(mktemp -d)"
trap 'rm -rf "${temporary_directory}"' EXIT HUP INT TERM

curl --fail --silent --show-error --location \
  "${base_url}/${version}/${binary}" \
  --output "${temporary_directory}/${binary}"
curl --fail --silent --show-error --location \
  "${base_url}/${version}/${binary}.sha256" \
  --output "${temporary_directory}/${binary}.sha256"

(
  cd "${temporary_directory}"
  sha256sum --check "${binary}.sha256"
)

install -d "${install_dir}"
install -m 0755 "${temporary_directory}/${binary}" "${install_dir}/secret-broker"
"${install_dir}/secret-broker" --version
