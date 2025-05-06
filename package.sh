#!/bin/bash

VERSION="v0.0.0"

while [[ $# -gt 0 ]]; do
  case $1 in
    --version)
      VERSION="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

if [[ -z "$VERSION" ]]; then
  echo "Usage: $0 --version <version>"
  exit 1
fi

cargo build --release

cp ./target/release/cql_lsp ./cql_lsp_bin

TAR_NAME="cql_lsp-${VERSION}.tar"
tar -cf "$TAR_NAME" cql_lsp_bin install.sh

echo "Created $TAR_NAME"
echo "Cleaning up..."

rm cql_lsp_bin
