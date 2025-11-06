# cqlls

[🇺🇸 English](README.md) | [🇯🇵 日本語](README_jap.md)

[![Crates.io](https://img.shields.io/crates/v/cql_lsp.svg)](https://crates.io/crates/cql_lsp)</br> 
[![Support me on Patreon](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Fshieldsio-patreon.vercel.app%2Fapi%3Fusername%3Dakzestia%26type%3Dpatrons&style=for-the-badge)](https://patreon.com/akzestia)

The 1nonly Open Source **language server** for CQL (Cassandra Query Language) ^_^

https://github.com/user-attachments/assets/780f9005-d571-489d-93e3-e61f91dcb0fe

# cqlls vs Corpo 

- Free
- Open Source language server (under MIT License)
- Aiming to provide the best experience
- Seamless Integration with Zed && Nvim
- Written in Rust :D

# Installation

Install Language Server binary using cargo
```sh
cargo install cql_lsp
```

Add env variables to your shell config

```sh
export PATH="$HOME/.cargo/bin:$PATH"

# Default values for LSP | Docker
export CQL_LSP_DB_URL="172.17.0.2"
export CQL_LSP_DB_PASSWD="cassandra"
export CQL_LSP_DB_USER="cassandra"
export CQL_LSP_ENABLE_LOGGING="false"
export CQL_LSP_TYPE_ALIGNMENT_OFFSET="7"
```

## License

This project is licensed under the [MIT License](LICENSE).
