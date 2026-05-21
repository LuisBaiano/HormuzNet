#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# HormuzNet — menu.sh
# Menu interativo para subir os componentes da infraestrutura distribuída.
# Detecta automaticamente se o ambiente usa 'docker compose' (plugin v2) ou
# 'docker-compose' (binário standalone v1), garantindo compatibilidade com
# diferentes versões de Docker presentes nos laboratórios da faculdade.
# Para cada opção do menu, invoca o gerador Python (generate_dynamic.py) para
# criar um docker-compose-temp.yml sob medida e o sobe com as flags corretas.
# ═══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

# ── Detecta docker compose (v2 plugin) ou docker-compose (v1 standalone) ────
if docker compose version &>/dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
elif command -v docker-compose &>/dev/null; then
    DOCKER_COMPOSE="docker-compose"
else
    echo -e "\e[1;31m[ERRO] Nenhuma versão do docker compose encontrada!\e[0m"
    echo "       Instale o Docker Compose antes de continuar."
    exit 1
fi
echo -e "\e[0;90m[INFO] Usando: $DOCKER_COMPOSE\e[0m"

# Função para exibir o cabeçalho
header() {
    clear
    echo -e "\e[1;36m╔══════════════════════════════════════════════════════════╗"
    echo -e "║              HormuzNet — Painel de Controle              ║"
    echo -e "╚══════════════════════════════════════════════════════════╝\e[0m"
    echo ""
}

# Pegar o IP local (padrão)
LOCAL_IP=$(hostname -I | awk '{print $1}')

while true; do
    header
    echo -e "\e[1;33mEscolha um componente para subir neste PC:\e[0m"
    echo "1) Subir Broker Líder (Centro/B9)"
    echo "2) Subir Brokers Adicionais (Seguidores)"
    echo "3) Subir Monitor"
    echo "4) Subir Drones"
    echo "5) Subir Sensores"
    echo "0) Sair"
    echo ""
    read -p "Opção: " OPTION

    case $OPTION in
        1)
            echo -e "\n\e[1;32m=> Iniciando o Broker Líder...\e[0m"
            python3 code/generate_dynamic.py --mode lider
            $DOCKER_COMPOSE -f docker-compose-temp.yml up -d --build
            echo -e "\n\e[1;32m[SUCESSO] Líder rodando! O IP deste Líder para os outros PCs é: \e[1;37m$LOCAL_IP\e[0m"
            read -p "Pressione Enter para continuar..."
            ;;
        2)
            echo -e "\n\e[1;32m=> Brokers Adicionais\e[0m"
            read -p "Qual é o IP do Líder? (Deixe em branco para usar $LOCAL_IP): " LIDER_IP
            LIDER_IP=${LIDER_IP:-$LOCAL_IP}
            read -p "Quantos brokers você quer subir neste PC? (1 a 8): " COUNT
            python3 code/generate_dynamic.py --mode brokers --count "$COUNT" --lider "$LIDER_IP"
            $DOCKER_COMPOSE -f docker-compose-temp.yml up -d --build
            echo -e "\n\e[1;32m[SUCESSO] Subidos $COUNT brokers apontando para o Líder $LIDER_IP!\e[0m"
            read -p "Pressione Enter para continuar..."
            ;;
        3)
            echo -e "\n\e[1;32m=> Monitor (Dashboard)\e[0m"
            read -p "Qual é o IP do Líder? (Deixe em branco para usar $LOCAL_IP): " LIDER_IP
            LIDER_IP=${LIDER_IP:-$LOCAL_IP}
            python3 code/generate_dynamic.py --mode monitor --lider "$LIDER_IP"
            $DOCKER_COMPOSE -f docker-compose-temp.yml up -d --build
            echo -e "\n\e[1;32m[SUCESSO] Monitor rodando! Acesse: http://localhost:8085 or http://$LOCAL_IP:8085\e[0m"
            read -p "Pressione Enter para continuar..."
            ;;
        4)
            echo -e "\n\e[1;32m=> Drones\e[0m"
            read -p "Qual é o IP do Líder? (Deixe em branco para usar $LOCAL_IP): " LIDER_IP
            LIDER_IP=${LIDER_IP:-$LOCAL_IP}
            read -p "Quantos Drones você quer subir?: " COUNT
            python3 code/generate_dynamic.py --mode drones --count "$COUNT" --lider "$LIDER_IP"
            $DOCKER_COMPOSE -f docker-compose-temp.yml up -d --build
            echo -e "\n\e[1;32m[SUCESSO] Subidos $COUNT Drones!\e[0m"
            read -p "Pressione Enter para continuar..."
            ;;
        5)
            echo -e "\n\e[1;32m=> Sensores (2 por setor)\e[0m"
            read -p "Qual é o IP do Líder? (Deixe em branco para usar $LOCAL_IP): " LIDER_IP
            LIDER_IP=${LIDER_IP:-$LOCAL_IP}
            read -p "Quantos setores (brokers) você quer cobrir com sensores? (1 a 9): " COUNT
            python3 code/generate_dynamic.py --mode sensores --count "$COUNT" --lider "$LIDER_IP"
            $DOCKER_COMPOSE -f docker-compose-temp.yml up -d --build
            echo -e "\n\e[1;32m[SUCESSO] Sensores criados!\e[0m"
            read -p "Pressione Enter para continuar..."
            ;;
        0)
            echo "Saindo..."
            exit 0
            ;;
        *)
            echo -e "\e[1;31mOpção inválida!\e[0m"
            sleep 2
            ;;
    esac
done
