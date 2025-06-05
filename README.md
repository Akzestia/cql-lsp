# cql-lsp

[🇺🇸 English](README.md) | [🇯🇵 日本語](README_jap.md)

[![Crates.io](https://img.shields.io/crates/v/cql_lsp.svg)](https://crates.io/crates/cql_lsp)

The 1nonly Open Source LSP implementation for CQL (Cassandra Query Language) ^_^

https://github.com/user-attachments/assets/780f9005-d571-489d-93e3-e61f91dcb0fe

# cql-lsp vs DataGrip (CQL Support)

| Feature                | cql-lsp (Open Source)              | DataGrip (JetBrains)            |
|------------------------|------------------------------------|----------------------------------|
| **License**            | MIT (Free)                         | Proprietary                      |
| **Pricing**            | $0                                 | $129/year (Personal)             |
| **CQL Completion**     | Full syntax + schema-aware         | Full syntax + schema-aware       |
| **Live Schema Sync**   | ✅ Yes                             | ✅ Yes                           |
| **Type Checking**      | ✅ Yes                             | ✅ Yes                           |
| **IDE Integration**    | Zed, Neovim, any LSP-compatible    | JetBrains IDEs only              |
| **Extensibility**      | Customizable Rust implementation   | Plugin system                    |

# Installation | Cargo 

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

# IDE Support

### [Zed](https://zed.dev/)
  - Integrated into [CQL](https://zed.dev/extensions?query=CQL) extension <br/>
### [Nvim](https://neovim.io/)
  - Requires manual configuration via [lsp-config](https://neovim.io/doc/user/lsp.html) <br/>

## License

This project is licensed under the [MIT License](LICENSE).
