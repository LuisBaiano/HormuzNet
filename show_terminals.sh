#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# HormuzNet — show_terminals.sh
# Script para abrir janelas de terminal dedicadas monitorando os logs de cada contêiner ativo.
# ═══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

# Lista todos os contêineres do HormuzNet ativos
CONTAINERS=$(docker ps --format "{{.Names}}" | grep -E "hormuznet_(broker|drone|sonar|meteo|radar|visual|boia)" || true)

if [ -z "$CONTAINERS" ]; then
    echo -e "\e[1;31m[ERRO] Nenhum contêiner do HormuzNet está em execução neste computador.\e[0m"
    exit 1
fi

echo -e "\e[1;36m=== Abrindo terminais para contêineres ativos... ===\e[0m"

# Tenta encontrar o emulador de terminal disponível
if command -v gnome-terminal &> /dev/null; then
    for c in $CONTAINERS; do
        echo "Abrindo terminal para $c..."
        gnome-terminal --title="$c" -- bash -c "docker logs -f $c" &
    done
elif command -v xterm &> /dev/null; then
    for c in $CONTAINERS; do
        echo "Abrindo terminal para $c..."
        xterm -title "$c" -e "docker logs -f $c" &
    done
elif command -v konsole &> /dev/null; then
    for c in $CONTAINERS; do
        echo "Abrindo terminal para $c..."
        konsole --title "$c" -e "docker logs -f $c" &
    done
elif command -v xfce4-terminal &> /dev/null; then
    for c in $CONTAINERS; do
        echo "Abrindo terminal para $c..."
        xfce4-terminal --title="$c" -e "docker logs -f $c" &
    done
else
    echo -e "\n\e[1;33m[AVISO] Nenhum emulador de terminal suportado foi encontrado (gnome-terminal, xterm, konsole, xfce4-terminal).\e[0m"
    echo "Você pode visualizar os logs manualmente em qualquer terminal executando:"
    echo "  docker logs -f <nome-do-conteiner>"
fi
