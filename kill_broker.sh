#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# HormuzNet — kill_broker.sh
# Script para listar brokers rodando e simular a queda de um nó.
# ═══════════════════════════════════════════════════════════════════════════════

echo -e "\e[1;36m=== Brokers Rodando Neste Computador ===\e[0m"

# Lista os containers filtrando pelos brokers
BROKERS=$(docker ps --format "{{.Names}}" | grep -i "hormuznet_broker")

if [ -z "$BROKERS" ]; then
    echo -e "\e[1;31mNenhum broker está rodando no momento.\e[0m"
    exit 0
fi

# Imprime a lista
docker ps --format "Nome: \e[1;33m{{.Names}}\e[0m | Status: {{.Status}}" | grep -i "hormuznet_broker"

echo ""
echo -e "Para testar a resiliência da rede, você pode derrubar um broker."
read -p "Digite o NOME do broker que deseja matar (ex: hormuznet_broker9) ou aperte ENTER para cancelar: " BROKER_NAME

if [ -n "$BROKER_NAME" ]; then
    echo -e "\n\e[1;31m[!] Derrubando o $BROKER_NAME...\e[0m"
    docker stop "$BROKER_NAME" > /dev/null
    docker rm "$BROKER_NAME" > /dev/null
    echo -e "\e[1;32m=> $BROKER_NAME foi destruído com sucesso!\e[0m"
else
    echo "Operação cancelada."
fi
