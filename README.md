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

# Installation | Cargo (Recommended)

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

# Installation | Source Code

> [!IMPORTANT]
> Please note that the source code may contain unstable **features**. <br/>
> It’s recommended to install from the **latest release** or via **cargo**. <br/>

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
  - Integrated with [CQL](https://zed.dev/extensions?query=CQL) extension <br/>
### [Nvim](https://neovim.io/)
  - Requires manual configuration via [lsp-config](https://neovim.io/doc/user/lsp.html) <br/>

## License

This project is licensed under the [MIT License](LICENSE).
