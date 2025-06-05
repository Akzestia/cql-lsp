# cql-lsp

[🇺🇸 English](README.md) | [🇯🇵 日本語](README_jap.md)

[![Crates.io](https://img.shields.io/crates/v/cql_lsp.svg)](https://crates.io/crates/cql_lsp)

CQL (Cassandra クエリ言語) 用の唯一のオープン ソース LSP ^_^

https://github.com/user-attachments/assets/780f9005-d571-489d-93e3-e61f91dcb0fe

# cql-lsp vs DataGrip (CQL サポート)

| 機能 　　　　　　　　　　　| cql-lsp (オープンソース) 　　　　　　　| DataGrip (JetBrains)　　　　　　　　|
|------------------------|------------------------------------|----------------------------------|
| **ライセンス**           | MIT (無料)                          | プロプライエタリ　　　　　　　　　　　|
| **価格**                | $0                                 | $129/年 (個人向け)                |
| **CQL 補完** 　　　　　　 | 完全な構文 + スキーマ対応              | 完全な構文 + スキーマ対応           |
| **ライブスキーマ同期** 　　| ✅ はい                             | ✅ はい                          |
| **型チェック** 　　　　　　| ✅ はい                             | ✅ はい                          |
| **IDE 統合** 　　　　　　 | Zed、Neovim、LSP 互換のあらゆる環境    | JetBrains IDE のみ               |
| **拡張性** 　　　　　　　　| カスタマイズ可能な Rust 実装           | プラグインシステム                 |

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

# IDE サポート

### [Zed](https://zed.dev/)
- [CQL](https://zed.dev/extensions?query=CQL) 拡張機能に統合されています <br/>
### [Nvim](https://neovim.io/)
- [lsp-config](https://neovim.io/doc/user/lsp.html) による手動設定が必要です <br/>


## ライセンス

このプロジェクトは[MITライセンス](LICENSE)に基づいてライセンスされています。
