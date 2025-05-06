# cql-lsp

[🇺🇸 English](README.md) | [🇯🇵 日本語](README_jap.md)

[![Crates.io](https://img.shields.io/crates/v/cql_lsp.svg)](https://crates.io/crates/cql_lsp)

The 1nonly Open Source LSP for CQL (Cassandra Query Language) ^_^

https://github.com/user-attachments/assets/555c8ff8-73cc-4659-a2a5-a8b308d9be63

---------------------------------------------------------

# Installation | Cargo 

Install LSP binary using cargo
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
```

# Installation | Source Code

Clone repo
```sh
git clone https://github.com/Akzestia/cql-lsp.git                                                    
cd cql-lsp
```

Run install script
```sh
sudo chmod +x ./install_src.sh
sudo -E ./install_src.sh
```

> [!IMPORTANT]  
> deploy.sh package.sh & install.sh are only used for development purposes

## License

This project is licensed under the [MIT License](LICENSE).
