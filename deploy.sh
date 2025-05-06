#!/bin/bash
# cargo build -j 16 --release
cargo build

if pgrep -x "cql_lsp" > /dev/null; then
    pkill -x "cql_lsp"
fi

# sudo cp ./target/release/cql_lsp /usr/bin
sudo cp ./target/debug/cql_lsp /usr/bin
