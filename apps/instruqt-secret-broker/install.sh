#!/bin/sh
set -eu

base_url="${SECRET_BROKER_BASE_URL:-https://tmprl-dem-cld-secret-broker-429214323166-us-west-1.s3.us-west-1.amazonaws.com/secret-broker}"
version="${SECRET_BROKER_VERSION:-main}"
install_dir="${SECRET_BROKER_INSTALL_DIR:-/usr/local/bin}"
signing_key_url="${SECRET_BROKER_SIGNING_KEY_URL:-${base_url}/trust/v1/signing-key.pem}"
signing_key_sha256="${SECRET_BROKER_SIGNING_KEY_SHA256:-cf0310e7827f2b444a876b06768e8e11bef5b2ad9d5f58f93e45c27674b2b594}"

download() {
  curl --fail --silent --show-error --location \
    --retry 10 \
    --retry-all-errors \
    --retry-delay 2 \
    --retry-max-time 60 \
    --connect-timeout 10 \
    "$1" \
    --output "$2"
}

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

download "${base_url}/${version}/${binary}" "${temporary_directory}/${binary}"
download "${base_url}/${version}/${binary}.sha256" "${temporary_directory}/${binary}.sha256"
download "${base_url}/${version}/${binary}.sig" "${temporary_directory}/${binary}.sig"
download "${signing_key_url}" "${temporary_directory}/signing-key.pem"

(
  cd "${temporary_directory}"
  sha256sum --check "${binary}.sha256"
)

actual_signing_key_sha256="$(
  openssl pkey \
    -pubin \
    -in "${temporary_directory}/signing-key.pem" \
    -outform DER \
    | sha256sum \
    | awk '{print $1}'
)"
if [ "${actual_signing_key_sha256}" != "${signing_key_sha256}" ]; then
  echo "Signing public key fingerprint mismatch" >&2
  exit 1
fi

openssl dgst -sha256 \
  -verify "${temporary_directory}/signing-key.pem" \
  -signature "${temporary_directory}/${binary}.sig" \
  -sigopt rsa_padding_mode:pss \
  -sigopt rsa_pss_saltlen:32 \
  "${temporary_directory}/${binary}"

install -d "${install_dir}"
install -m 0755 "${temporary_directory}/${binary}" "${install_dir}/secret-broker"
"${install_dir}/secret-broker" --version
