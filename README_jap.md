# cql-lsp

[🇺🇸 English](README.md) | [🇯🇵 日本語](README_jap.md)

[![Crates.io](https://img.shields.io/crates/v/cql_lsp.svg)](https://crates.io/crates/cql_lsp)

CQL (Cassandra クエリ言語) 用の唯一のオープン ソース LSP ^_^

https://github.com/user-attachments/assets/555c8ff8-73cc-4659-a2a5-a8b308d9be63

---------------------------------------------------------

# インストール｜Cargo 

カーゴを使ってLSPバイナリをインストールする
```sh
cargo install cql_lsp
```

シェルの設定に環境変数を追加する

```sh
export PATH="$HOME/.cargo/bin:$PATH"

# LSP｜Docker 用のデフォルト値
export CQL_LSP_DB_URL="172.17.0.2"
export CQL_LSP_DB_PASSWD="cassandra"
export CQL_LSP_DB_USER="cassandra"
export CQL_LSP_ENABLE_LOGGING="false"
```

# インストール｜ソース・コード

クローン・リポジトリ
```sh
git clone https://github.com/Akzestia/cql-lsp.git                                                    
cd cql-lsp
```

インストールスクリプトを実行する
```sh
sudo chmod +x ./install_src.sh
sudo -E ./install_src.sh
```

> [!IMPORTANT]  
> deploy.sh package.sh & install.sh は開発目的でのみ使用されます。

## ライセンス

このプロジェクトは[MITライセンス](LICENSE)に基づいてライセンスされています。
