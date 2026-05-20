#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════════════════
# HormuzNet — eliminar.sh
# Script unificado para listar e parar contêineres ou limpar o ambiente.
# ═══════════════════════════════════════════════════════════════════════════════

echo -e "\e[1;36m=== Contêineres Ativos do HormuzNet ===\e[0m"

# Busca contêineres rodando com "hormuznet" no nome
mapfile -t CONTAINERS < <(docker ps --format "{{.Names}}" | grep -i "hormuznet" | sort)

if [ ${#CONTAINERS[@]} -eq 0 ]; then
    echo -e "\e[1;31mNenhum contêiner do HormuzNet está rodando no momento.\e[0m"
    echo -e "\nQuer limpar resíduos do ambiente mesmo assim? (s/n)"
    read -r RESIDUAL
    if [ "$RESIDUAL" = "s" ] || [ "$RESIDUAL" = "S" ]; then
        echo -e "\n\e[1;31m[!] Limpando processos Go...\e[0m"
        pkill -9 -f "go run ./cmd/|.*_main|echo '--- encerrado ---'" 2>/dev/null || true
        echo -e "\e[1;32m=> Processos limpos!\e[0m"
    fi
    exit 0
fi

# Exibe a lista numerada
for i in "${!CONTAINERS[@]}"; do
    status=$(docker ps -f "name=${CONTAINERS[$i]}" --format "{{.Status}}")
    echo -e "  \e[1;33m$((i+1))\e[0m) ${CONTAINERS[$i]} ($status)"
done

echo ""
echo -e "Digite os números dos contêineres que deseja parar (ex: 1 3 5),"
echo -e "ou digite '\e[1;31mtudo\e[0m' para parar e remover todo o ambiente, ou ENTER para cancelar:"
read -r SELECAO

if [ -z "$SELECAO" ]; then
    echo "Operação cancelada."
    exit 0
fi

if [ "$SELECAO" = "tudo" ] || [ "$SELECAO" = "TUDO" ]; then
    echo -e "\n\e[1;31m[!] Parando e removendo todos os contêineres e limpando processos Go...\e[0m"
    docker rm -f $(docker ps -aq) 2>/dev/null || true
    pkill -9 -f "go run ./cmd/|.*_main|echo '--- encerrado ---'" 2>/dev/null || true
    echo -e "\e[1;32m=> Todo o ambiente do HormuzNet foi limpo com sucesso!\e[0m"
    exit 0
fi

# Parar os contêineres selecionados
for num in $SELECAO; do
    # Verifica se é um número inteiro válido
    if [[ "$num" =~ ^[0-9]+$ ]] && [ "$num" -ge 1 ] && [ "$num" -le "${#CONTAINERS[@]}" ]; then
        idx=$((num-1))
        target="${CONTAINERS[$idx]}"
        echo -e "\n\e[1;31m[!] Parando o contêiner $target...\e[0m"
        docker stop "$target" > /dev/null
        echo -e "\e[1;32m=> $target foi parado com sucesso!\e[0m"
    else
        echo -e "\e[1;31m[Erro] Opção inválida: '$num'\e[0m"
    fi
done
