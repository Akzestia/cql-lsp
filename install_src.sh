#!/bin/bash
set -e

cargo build --release

INSTALL_PATH="/usr/local/bin"
BINARY_NAME="./target/release/cql_lsp"

if [[ "$EUID" -ne 0 ]]; then
 echo "Please run as root using sudo"
 exit 1
fi

TARGET_USER=${SUDO_USER:-$USER}
TARGET_HOME=$(eval echo "~$TARGET_USER")
TARGET_SHELL=$(getent passwd "$TARGET_USER" | cut -d: -f7)
SHELL_NAME=$(basename "$TARGET_SHELL")

case "$SHELL_NAME" in
 zsh) CONFIG_FILE="$TARGET_HOME/.zshrc" ;;
 bash) CONFIG_FILE="$TARGET_HOME/.bashrc" ;;
 *)
   echo "Unsupported shell: $SHELL_NAME"
   exit 1
   ;;
esac

echo "Installing $BINARY_NAME to $INSTALL_PATH"
cp "$BINARY_NAME" "$INSTALL_PATH/"
chmod +x "$INSTALL_PATH/cql_lsp"

echo "Adding environment variables to $CONFIG_FILE"
{
 echo ""
 echo "# cql_lsp configuration"
 echo 'export CQL_LSP_DB_URL="127.0.0.1"'
 echo 'export CQL_LSP_DB_PASSWD="cassandra"'
 echo 'export CQL_LSP_DB_USER="cassandra"'
 echo 'export CQL_LSP_ENABLE_LOGGING="false"'
} >> "$CONFIG_FILE"

echo "Installation complete for user $TARGET_USER."
echo "Please restart your terminal or run: source $CONFIG_FILE"
