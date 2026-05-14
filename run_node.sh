#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# Script de Fácil Execução Distribuída (Para rodar em outras máquinas)
#
# Este script prepara o ambiente e executa os componentes do HormuzNet.
# Ele verifica a instalação do Go, compila o projeto e inicia o nó desejado.
# ═══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

# ── Configurações ─────────────────────────────────────────────────────────────
GO_VERSION="1.21.0"
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$([[ $(uname -m) == "x86_64" ]] && echo "amd64" || echo "arm64")

print_help() {
    echo "Uso: ./run_node.sh [OPÇÕES]"
    echo ""
    echo "Opções:"
    echo "  -m, --mode <modo>    Modo de execução: all | broker | drone | sensor (padrão: all)"
    echo "  -i, --ip <ip>        IP da máquina principal onde estão os Brokers (padrão: localhost)"
    echo "  -h, --help           Mostra esta mensagem de ajuda"
    echo ""
    echo "Exemplos:"
    echo "  ./run_node.sh                               # Roda tudo localmente"
    echo "  ./run_node.sh --ip 192.168.1.50 --mode drone  # Roda drones conectando no IP especificado"
}

MODE="all"
IP="localhost"

# Parse args
while [[ $# -gt 0 ]]; do
  case $1 in
    -m|--mode)
      MODE="$2"
      shift 2
      ;;
    -i|--ip)
      IP="$2"
      shift 2
      ;;
    -h|--help)
      print_help
      exit 0
      ;;
    *)
      echo "Argumento desconhecido: $1"
      print_help
      exit 1
      ;;
  esac
done

# ── Verificação de Dependências ───────────────────────────────────────────────
echo "Verificando dependências..."

if ! command -v go &>/dev/null; then
    echo "Go não está instalado. Deseja tentar instalar agora? (Requer privilégios sudo) [s/N]"
    read -r resp
    if [[ "$resp" == "s" || "$resp" == "S" ]]; then
        echo "Baixando Go ${GO_VERSION}..."
        wget -q "https://go.dev/dl/go${GO_VERSION}.${OS}-${ARCH}.tar.gz" -O /tmp/go.tar.gz
        echo "Extraindo e instalando..."
        sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf /tmp/go.tar.gz
        export PATH=$PATH:/usr/local/go/bin
        echo "Go instalado com sucesso!"
    else
        echo "Por favor, instale o Go e tente novamente: https://go.dev/doc/install"
        exit 1
    fi
else
    echo "Go encontrado: $(go version)"
fi

# ── Preparação do Projeto ─────────────────────────────────────────────────────
echo "Baixando dependências do projeto..."
go mod tidy || { echo "Erro ao baixar dependências do Go."; exit 1; }

echo "Compilando o projeto..."
go build ./... || { echo "Erro na compilação."; exit 1; }

# ── Execução ──────────────────────────────────────────────────────────────────
echo "Iniciando no modo '$MODE' apontando para IP '$IP'..."
chmod +x start.sh
./start.sh "$IP" "$MODE"
